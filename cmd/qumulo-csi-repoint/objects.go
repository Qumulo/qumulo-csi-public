// Copyright (c) 2026 Qumulo, Inc. All rights reserved.
// Use of this software is governed by the Qumulo CSI Driver License
// found in the LICENSE file at the root of this repository.
// SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sjson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
)

// Reading and writing Kubernetes objects.
//
// Everything this command emits is a manifest a human can read, review and
// `kubectl apply`, because that is what a DR procedure is reviewed as. The
// cluster writes in cluster.go go through the same structs, so what
// `--dry-run` prints and what `--dry-run=false` applies are the same objects
// built by the same code -- not two renderings that can drift.

// objectStream is a set of PVs, PVCs and VolumeReplications: everything the
// four subcommands read, whether it came from files, from stdin, or from the
// cluster (see failoverObjects).
type objectStream struct {
	Volumes []*corev1.PersistentVolume
	Claims  []*corev1.PersistentVolumeClaim
	// Replications are csi-addons VolumeReplication objects, which `failover`
	// pairs. They are not in client-go's scheme -- the CRD belongs to another
	// project -- so they are recognised by apiVersion and read as unstructured
	// rather than being refused as an unsupported kind.
	Replications []volumeReplication
	// ReplicationClasses are the cluster-scoped VolumeReplicationClasses, read
	// for one parameter: a class with `replicationScope: group` is driven as a
	// unit, and its member volumes have no object of their own.
	ReplicationClasses []volumeReplicationClass
	// Pods and Workloads are step 0's input: who is writing to the source, and
	// what has to be scaled to zero to stop them (see quiesce.go).
	Pods      []corev1.Pod
	Workloads []workload
	// PodsObserved says a pod list was actually read. Distinct from an empty
	// Pods, because "nothing is running" and "nobody looked" are the same value
	// and opposite answers when the next step freezes production.
	PodsObserved bool
}

// merge folds one stream into another, which is what the List wrapper and a
// repeated -f both need.
func (o *objectStream) merge(in objectStream) {
	o.Volumes = append(o.Volumes, in.Volumes...)
	o.Claims = append(o.Claims, in.Claims...)
	o.Replications = append(o.Replications, in.Replications...)
	o.ReplicationClasses = append(o.ReplicationClasses, in.ReplicationClasses...)
	o.Pods = append(o.Pods, in.Pods...)
	o.Workloads = append(o.Workloads, in.Workloads...)
	o.PodsObserved = o.PodsObserved || in.PodsObserved
}

// decodeObjects reads a YAML or JSON stream of Kubernetes objects.
//
// Multi-document YAML is the normal shape here: `kubectl get pv -o yaml`
// produces a List, and a hand-assembled input is usually several documents in
// one file. Both are accepted, plus a bare single object.
//
// A List is read one item at a time and each item is decoded and dropped
// before the next is read, so what stays in memory is the decoded objects
// and one item's text -- never the whole input, nor a second copy of every
// item (reading the input whole put peak memory at about 6 times a
// JSON input and 30 times a YAML one).
func decodeObjects(r io.Reader) (objectStream, error) {
	var out objectStream

	input := bufio.NewReaderSize(r, 64<<10)
	first, err := firstNonSpace(input)
	if errors.Is(err, io.EOF) {
		return out, nil
	}
	if err != nil {
		return out, fmt.Errorf("reading input: %w", err)
	}
	// `kubectl get -o json` starts with `{"`; YAML -- what `-o yaml` and
	// hand-written files are -- does not, unless it is a flow mapping such as
	// `{apiVersion: v1, ...}`, which the reader this replaces also read as
	// YAML once it failed as JSON. JSON is read as JSON throughout: a stream
	// that turns into YAML part way is refused, where that reader would have
	// guessed again.
	if first == '{' && opensJSONObject(input) {
		if err := decodeJSONStream(input, &out); err != nil {
			var syntax *json.SyntaxError
			if errors.As(err, &syntax) {
				return out, fmt.Errorf("%w; a JSON input has to be JSON throughout -- put YAML "+
					"documents in a file of their own (-f is repeatable)", err)
			}
			return out, err
		}
		return out, nil
	}
	return out, decodeYAMLStream(input, &out)
}

// opensJSONObject reports whether the `{` at the head of r is followed by a
// quoted key or a `}`, which is what tells a JSON object from a YAML flow
// mapping with bare keys. Nothing is consumed.
func opensJSONObject(r *bufio.Reader) bool {
	for n := 64; ; n *= 2 {
		peeked, err := r.Peek(n)
		for _, b := range peeked[1:] {
			if !unicode.IsSpace(rune(b)) {
				return b == '"' || b == '}'
			}
		}
		if err != nil || n >= r.Size() {
			return true // nothing but white space after it: let JSON say what is wrong
		}
	}
}

// firstNonSpace returns the first byte of r that is not white space,
// without consuming it.
func firstNonSpace(r *bufio.Reader) (byte, error) {
	for {
		b, err := r.Peek(1)
		if err != nil {
			return 0, err
		}
		if !unicode.IsSpace(rune(b[0])) {
			return b[0], nil
		}
		if _, err := r.Discard(1); err != nil {
			return 0, err
		}
	}
}

// listItems collects the items of one List while the List is still being
// read. Its kind is only known once the whole object has been read --
// kubectl prints `items` before `kind` -- so the items are decoded into a
// stream of their own and only joined to the output once the object turns
// out to be a v1 List. An item that fails to decode is remembered, not
// returned at once, for the same reason: the error that matters for an
// object that is not a List is that it is not one.
type listItems struct {
	seen   bool
	stream objectStream
	err    error
}

// add decodes one item, unless an earlier one has already failed.
func (l *listItems) add(raw []byte) {
	if l.err == nil {
		l.err = decodeDocument(raw, &l.stream)
	}
}

// finish settles an object whose top-level fields other than `items` are
// head (JSON). A v1 List contributes its items; anything else is decoded
// whole, with its items left out, and so refused for what it is -- a
// PersistentVolumeList, say -- exactly as when it was read in one piece.
func (l *listItems) finish(head []byte, out *objectStream) error {
	if l.seen {
		var kind metav1.TypeMeta
		if json.Unmarshal(head, &kind) == nil && kind.APIVersion == "v1" && kind.Kind == "List" {
			// The List itself, its items left out, is decoded as before, so
			// that what was refused about it still is.
			if err := decodeDocument(head, out); err != nil {
				return err
			}
			if l.err != nil {
				return l.err
			}
			out.merge(l.stream)
			return nil
		}
	}
	return decodeDocument(head, out)
}

// decodeJSONStream reads one or more JSON objects, streaming the items of
// each List.
func decodeJSONStream(r io.Reader, out *objectStream) error {
	decoder := json.NewDecoder(r)
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading input: %w", err)
		}
		if token != json.Delim('{') {
			return fmt.Errorf("reading input: expected a JSON object, found %v", token)
		}
		if err := decodeJSONObject(decoder, out); err != nil {
			return err
		}
	}
}

// decodeJSONObject reads the rest of an object whose `{` has been read. Its
// `items` array, if it has one, is decoded an element at a time; every other
// field is kept as it is -- they are a few short ones on a List, and the
// whole of a single object.
func decodeJSONObject(decoder *json.Decoder, out *objectStream) error {
	fields := map[string]json.RawMessage{}
	var items listItems

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("reading input: %w", err)
		}
		key, _ := token.(string)

		// Matched as encoding/json matches a field, ignoring case: the
		// reader this replaces unmarshalled the List with it.
		if strings.EqualFold(key, "items") {
			if items.seen || hasItemsField(fields) {
				return errRepeatedItems
			}
			token, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("reading input: %w", err)
			}
			if _, isDelim := token.(json.Delim); !isDelim {
				// A scalar -- null, or something no List has -- is kept as
				// it is, and the object is judged whole like any other.
				value, err := json.Marshal(token)
				if err != nil {
					return fmt.Errorf("reading input: %w", err)
				}
				fields[key] = value
				continue
			}
			if token != json.Delim('[') {
				return fmt.Errorf("reading input: items is an object, not a list")
			}
			items.seen = true
			for decoder.More() {
				var raw json.RawMessage
				if err := decoder.Decode(&raw); err != nil {
					return fmt.Errorf("reading input: %w", err)
				}
				if !bytes.Equal(raw, []byte("null")) {
					items.add(raw)
				}
			}
			if _, err := decoder.Token(); err != nil { // the closing ]
				return fmt.Errorf("reading input: %w", err)
			}
			continue
		}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return fmt.Errorf("reading input: %w", err)
		}
		fields[key] = value
	}
	if _, err := decoder.Token(); err != nil { // the closing }
		return fmt.Errorf("reading input: %w", err)
	}

	if items.seen {
		fields["items"] = json.RawMessage("[]")
	}
	head, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("reading input: %w", err)
	}
	return items.finish(head, out)
}

// hasItemsField reports whether fields already holds an `items`, in any case.
func hasItemsField(fields map[string]json.RawMessage) bool {
	for key := range fields {
		if strings.EqualFold(key, "items") {
			return true
		}
	}
	return false
}

// decodeYAMLStream reads a multi-document YAML stream, streaming the items of
// each List.
//
// A List as kubectl prints it is one document, and converting that document
// to JSON in one piece builds a generic tree of the whole input -- about 30
// times its size. So the `items:` sequence of a document is cut into
// its items by their indentation, and each item is converted on its own. The
// rest of the document -- apiVersion, kind, metadata -- is converted as
// before. Anything that does not have that shape (an inline `items: []`, a
// quoted key) is read whole, as before. Streaming is an economy, not a
// different reading, with two exceptions, both refused with what to do: an
// anchor defined in one item and used in another, and two `items` keys.
func decodeYAMLStream(r *bufio.Reader, out *objectStream) error {
	document := yamlDocument{seqIndent: -1}
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			// Documents are split on whole lines, as the API machinery's
			// reader splits them; only then is a line cut at its bare
			// carriage returns.
			separator, err := documentSeparator(line)
			if err != nil {
				return fmt.Errorf("reading input: %w", err)
			}
			if separator && document.lines > 0 {
				if err := document.finish(out); err != nil {
					return err
				}
				document = yamlDocument{seqIndent: -1}
			} else {
				// A separator that opens a document is part of it, as it was
				// to the reader this replaces: the YAML parser reads it as
				// the document's start marker.
				for _, line := range splitBareCR(line) {
					if err := document.add(line); err != nil {
						return err
					}
				}
			}
		}
		if errors.Is(err, io.EOF) {
			return document.finish(out)
		}
		if err != nil {
			return fmt.Errorf("reading input: %w", err)
		}
	}
}

// splitBareCR splits a line at every carriage return not followed by a line
// feed, which YAML also reads as a line break.
func splitBareCR(line []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i := 0; i+1 < len(line); i++ {
		if line[i] == '\r' && line[i+1] != '\n' {
			lines = append(lines, line[start:i+1])
			start = i + 1
		}
	}
	return append(lines, line[start:])
}

// documentSeparator reports a `---` line, which may carry a comment and
// nothing else -- the rule of the API machinery's own YAML reader, which
// splits documents for kubectl too.
func documentSeparator(line []byte) (bool, error) {
	if !bytes.HasPrefix(line, []byte("---")) {
		return false, nil
	}
	rest := bytes.TrimSpace(line[3:])
	if len(rest) > 0 && rest[0] != '#' {
		return false, fmt.Errorf("invalid Yaml document separator: %s", rest)
	}
	return true, nil
}

// yamlDocument is one YAML document being read a line at a time.
type yamlDocument struct {
	// lines counts the lines read into the document, blank ones included.
	lines int
	// head is every line outside the top-level `items:` sequence.
	head bytes.Buffer
	// inItems is set from a top-level `items:` line until the sequence ends.
	inItems bool
	// seqIndent is the column of the sequence's `-`, -1 until its first item.
	seqIndent int
	// item is the text of the item being read, and converted its JSON once
	// itemComplete has parsed it.
	item      bytes.Buffer
	converted []byte
	items     listItems
}

// add takes one line, newline included.
func (d *yamlDocument) add(line []byte) error {
	d.lines++
	if d.inItems {
		indent, content := yamlIndent(line)
		if len(content) == 0 || content[0] == '#' {
			// A blank or comment line belongs to whatever it sits in.
			if d.item.Len() > 0 {
				d.item.Write(line)
				return nil
			}
			return d.dropped(line)
		}
		startsItem := bytes.Equal(content, []byte("-")) || bytes.HasPrefix(content, []byte("- ")) ||
			bytes.HasPrefix(content, []byte("-\t"))
		switch {
		case d.seqIndent < 0 && startsItem:
			d.seqIndent = indent
			d.items.seen = true
			d.item.Write(line)
			return nil
		case d.seqIndent < 0:
			// `items:` holds something other than a block sequence: read
			// the document whole, as before.
			d.inItems = false
			d.head.WriteString("items:\n")
		case indent > d.seqIndent:
			d.item.Write(line)
			return nil
		case indent == d.seqIndent && startsItem:
			if err := d.flushItem(); err != nil {
				return err
			}
			d.item.Write(line)
			return nil
		case !d.itemComplete():
			// The item is still open -- a flow collection or a quoted
			// scalar running on -- so the line is its, whatever its
			// indentation.
			d.item.Write(line)
			return nil
		case indent == 0:
			// Back at the document's top level: the sequence has ended.
			if err := d.flushItem(); err != nil {
				return err
			}
			d.inItems = false
		default:
			// Inside `items:` but not inside an item: not YAML, whole or
			// cut up, so it is refused rather than read some other way.
			return fmt.Errorf("reading input: yaml: line %d of a document is indented less than "+
				"the items: list it is in", d.lines)
		}
	}

	if isItemsKey(line) {
		if d.items.seen {
			return errRepeatedItems
		}
		d.inItems = true
		d.seqIndent = -1
		return d.dropped(line)
	}
	d.head.Write(line)
	return nil
}

// dropped puts a line the document's parse never sees -- `items:` itself, a
// comment before the first item -- through the parser on its own, so that
// what the parser refused in it (a control character, invalid UTF-8) is
// still refused.
func (d *yamlDocument) dropped(line []byte) error {
	if _, err := yamlToJSON(line); err != nil {
		return fmt.Errorf("reading input: %w", err)
	}
	return nil
}

// isItemsKey reports a top-level `items:` line with no value on it.
func isItemsKey(line []byte) bool {
	rest, ok := bytes.CutPrefix(line, []byte("items:"))
	if !ok {
		return false
	}
	trimmed := bytes.TrimSpace(rest)
	// A comment only after white space: `items:#` is not `items:`.
	return len(trimmed) == 0 || (trimmed[0] == '#' && len(rest) > len(bytes.TrimLeft(rest, " \t")))
}

// yamlIndent returns a line's leading spaces and what follows them, trimmed.
func yamlIndent(line []byte) (int, []byte) {
	indent := 0
	for indent < len(line) && line[indent] == ' ' {
		indent++
	}
	return indent, bytes.TrimSpace(line[indent:])
}

// itemComplete reports whether the item read so far parses on its own. It
// is asked only when a line would end the item at a lower indentation, so
// it costs one parse per List, not one per line; the parse is kept for
// flushItem.
func (d *yamlDocument) itemComplete() bool {
	if d.item.Len() == 0 {
		return true
	}
	converted, err := d.convertItem()
	if err != nil {
		return false
	}
	d.converted = converted
	return true
}

// convertItem converts the item read so far to JSON.
func (d *yamlDocument) convertItem() ([]byte, error) {
	// Converted under its own `items:` key, at the indentation it was read
	// at, so that no line has to be re-indented: the item is parsed in the
	// same position it had in the document.
	var wrapped bytes.Buffer
	wrapped.Grow(len("items:\n") + d.item.Len())
	wrapped.WriteString("items:\n")
	wrapped.Write(d.item.Bytes())
	return yamlToJSON(wrapped.Bytes())
}

// flushItem converts the item read so far and decodes it.
func (d *yamlDocument) flushItem() error {
	if d.item.Len() == 0 {
		return nil
	}
	defer d.item.Reset()

	converted, err := d.converted, error(nil)
	d.converted = nil
	if converted == nil {
		converted, err = d.convertItem()
	}
	if err != nil {
		if strings.Contains(err.Error(), "unknown anchor") {
			return fmt.Errorf("reading input: %w; a YAML anchor defined in one List item and used "+
				"in another is not supported -- expand it, or pass the input as `-o json`", err)
		}
		return fmt.Errorf("reading input: %w", err)
	}
	var one struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(converted, &one); err != nil || len(one.Items) != 1 {
		return fmt.Errorf("reading input: a List item did not read as one object")
	}
	if !bytes.Equal(one.Items[0], []byte("null")) {
		d.items.add(one.Items[0])
	}
	return nil
}

// finish decodes the document once all of it has been read.
func (d *yamlDocument) finish(out *objectStream) error {
	if d.inItems {
		if err := d.flushItem(); err != nil {
			return err
		}
		if !d.items.seen {
			d.head.WriteString("items:\n") // a bare `items:`, which is null
		}
		d.inItems = false
	}
	head, err := yamlToJSON(d.head.Bytes())
	if err != nil {
		return fmt.Errorf("reading input: %w", err)
	}
	if d.items.seen {
		// The streamed items stand in for the document's `items`; one
		// spelled some other way (`items :`, quoted) as well is refused.
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(head, &fields); err != nil && head != nil {
			return fmt.Errorf("reading input: %w", err)
		}
		if _, repeated := fields["items"]; repeated {
			return errRepeatedItems
		}
		if fields == nil {
			fields = map[string]json.RawMessage{}
		}
		fields["items"] = json.RawMessage("[]")
		if head, err = json.Marshal(fields); err != nil {
			return fmt.Errorf("reading input: %w", err)
		}
	}
	if head == nil {
		// An empty document, which a trailing `---` produces.
		return nil
	}
	return d.items.finish(head, out)
}

// errRepeatedItems refuses an object with two `items`: which one a List is
// made of is a guess, and this command does not guess at which volumes it
// fails over.
var errRepeatedItems = errors.New("reading input: an object has more than one items key; " +
	"which list it holds is ambiguous")

// yamlToJSON converts one YAML document to JSON the way the stream decoder
// this replaces did, so that a document reads the same whole or in parts. It
// returns nil for an empty or null document.
func yamlToJSON(document []byte) ([]byte, error) {
	var raw runtime.RawExtension
	err := utilyaml.NewYAMLToJSONDecoder(bytes.NewReader(document)).Decode(&raw)
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	return raw.Raw, err
}

// decodeDocument decodes one object -- a document of the input, or an item of
// a List -- and appends it to out.
func decodeDocument(raw []byte, out *objectStream) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		// An empty document, which a trailing `---` produces.
		return nil
	}

	// The CRD first: client-go's scheme has never heard of it, so the
	// universal deserializer below would refuse the document outright.
	// Its kind is read from the TypeMeta alone; decoding every document
	// whole as unstructured first, and then again typed, put peak memory
	// at about 20 times the input.
	var kind metav1.TypeMeta
	if err := json.Unmarshal(raw, &kind); err == nil &&
		strings.HasPrefix(kind.APIVersion, volumeReplicationGVR.Group+"/") {
		var probe unstructured.Unstructured
		if err := probe.UnmarshalJSON(raw); err != nil {
			return fmt.Errorf("decoding %s: %w", kind.Kind, err)
		}
		switch probe.GetKind() {
		case volumeReplicationKind:
			vr, err := volumeReplicationFrom(&probe)
			if err != nil {
				return err
			}
			out.Replications = append(out.Replications, vr)
		case volumeReplicationClassKind:
			class, err := volumeReplicationClassFrom(&probe)
			if err != nil {
				return err
			}
			out.ReplicationClasses = append(out.ReplicationClasses, class)
		default:
			return fmt.Errorf("unsupported %s object %q; this command reads %s and %s",
				volumeReplicationGVR.Group, probe.GetKind(),
				volumeReplicationKind, volumeReplicationClassKind)
		}
		return nil
	}

	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(raw, nil, nil)
	if err != nil {
		return fmt.Errorf("decoding object: %w", err)
	}

	switch typed := obj.(type) {
	case *corev1.PersistentVolume:
		trimKept(&typed.ObjectMeta)
		typed.Status = corev1.PersistentVolumeStatus{}
		out.Volumes = append(out.Volumes, typed)
	case *corev1.PersistentVolumeClaim:
		trimKept(&typed.ObjectMeta)
		typed.Status = corev1.PersistentVolumeClaimStatus{}
		out.Claims = append(out.Claims, typed)
	case *corev1.Pod:
		trimKept(&typed.ObjectMeta)
		// A pod in the input is a writer to stop before the promote. One of
		// them marks the stream as having a pod list; a file that holds
		// *none* leaves the flag false, because "nothing is running" and
		// "nobody looked" are the same value and opposite answers when the
		// next step freezes production.
		out.Pods = append(out.Pods, *typed)
		out.PodsObserved = true
	case *appsv1.Deployment:
		out.Workloads = append(out.Workloads, workloadFrom(kindDeployment, &typed.ObjectMeta,
			typed.Spec.Replicas))
	case *appsv1.StatefulSet:
		out.Workloads = append(out.Workloads, workloadFrom(kindStatefulSet, &typed.ObjectMeta,
			typed.Spec.Replicas))
	case *appsv1.ReplicaSet:
		out.Workloads = append(out.Workloads, workloadFrom(kindReplicaSet, &typed.ObjectMeta,
			typed.Spec.Replicas))
	case *corev1.List:
		return decodeList(typed, out)
	default:
		// Named rather than ignored: silently dropping an object the
		// operator piped in is how a DR run ends up half done.
		return fmt.Errorf("unsupported object %T; this tool reads PersistentVolumes, "+
			"PersistentVolumeClaims, Pods, Deployments, StatefulSets and ReplicaSets", obj)
	}
	return nil
}

// trimKept drops what no subcommand reads from an object it keeps until
// exit: managed fields, and every annotation but this driver's own (a
// `last-applied-configuration` alone is a second copy of the object). Every
// object is held for the whole run, so this is most of what a large input
// costs once it is read. Nothing read is ever written back: the
// objects this tool creates are built new, and its writes are patches.
func trimKept(meta *metav1.ObjectMeta) {
	meta.ManagedFields = nil
	for key := range meta.Annotations {
		if !strings.HasPrefix(key, driverAnnotationPrefix) {
			delete(meta.Annotations, key)
		}
	}
	if len(meta.Annotations) == 0 {
		meta.Annotations = nil
	}
}

// driverAnnotationPrefix is the prefix of every annotation this tool reads.
const driverAnnotationPrefix = "csi.qumulo.com/"

// decodeList unpacks the `kubectl get -o yaml` List wrapper into out.
func decodeList(list *corev1.List, out *objectStream) error {
	for i := range list.Items {
		// Read in place: a string copy of every item was a second copy of
		// the whole list, and a stream decoder per item most of the rest.
		if err := decodeDocument(list.Items[i].Raw, out); err != nil {
			return err
		}
	}
	return nil
}

// readObjectFiles decodes every named file, with "-" meaning stdin.
func readObjectFiles(paths []string, stdin io.Reader) (objectStream, error) {
	var out objectStream

	for _, path := range paths {
		var (
			reader io.Reader
			closer io.Closer
		)
		if path == "-" {
			reader = stdin
		} else {
			file, err := os.Open(path) //nolint:gosec // an operator's own manifest path
			if err != nil {
				return out, fmt.Errorf("opening %s: %w", path, err)
			}
			reader, closer = file, file
		}

		stream, err := decodeObjects(reader)
		if closer != nil {
			_ = closer.Close()
		}
		if err != nil {
			return out, fmt.Errorf("%s: %w", path, err)
		}

		out.merge(stream)
	}

	return out, nil
}

// yamlEncoder writes Kubernetes objects as the YAML `kubectl apply` reads.
//
// The API machinery's own serializer, not a struct-tag YAML marshaller: it
// stamps apiVersion and kind from the scheme, which is what makes the output
// applicable rather than merely YAML-shaped.
var yamlEncoder = k8sjson.NewSerializerWithOptions(
	k8sjson.DefaultMetaFactory, scheme.Scheme, scheme.Scheme,
	k8sjson.SerializerOptions{Yaml: true, Pretty: false},
)

// writeManifest writes objects as a multi-document YAML stream.
func writeManifest(w io.Writer, objects ...runtime.Object) error {
	for _, obj := range objects {
		if _, err := fmt.Fprintln(w, "---"); err != nil {
			return err
		}
		if err := yamlEncoder.Encode(obj, w); err != nil {
			return fmt.Errorf("encoding %T: %w", obj, err)
		}
	}
	return nil
}
