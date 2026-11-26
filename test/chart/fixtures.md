# Chart golden fixtures

Each `golden/<name>.yaml` is the committed output of `helm template` for the
value set of the same name in `chart_golden_test.go`.

They exist so a change to a template shows up as a reviewable diff. A tweak to
`_helpers.tpl` can silently alter a `securityContext`, an RBAC verb or a mount
propagation mode, and none of that is visible in a diff of the template alone.

Regenerate after an intentional change:

    make helm-golden-update

Then read the diff before committing it -- that diff is the whole point.
