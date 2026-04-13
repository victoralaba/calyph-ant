# Team vs Workspace (Calyphant)

## Workspace
- A **workspace** is the tenancy boundary (data + membership + billing tier context).
- Every collaborative action is scoped to a workspace ID.
- Limits such as members, backups, and query caps are resolved from the workspace billing tier.
- Subdomain identity (for Team+) is assigned to a workspace.

In short: **Workspace = container/boundary**.

## Team
- A **team** is the group of users (owner/admin/editor/viewer) inside a workspace.
- Roles decide *who can do what* inside that workspace.
- Team membership can change without changing the workspace itself.

In short: **Team = people + permissions inside a workspace**.

## Practical difference
- You can have a workspace with 1 person (still a workspace, not really a team workflow yet).
- As soon as you add members, you have a team operating inside that workspace.
- Billing plans like `team` / `mega_team` upgrade the **workspace capabilities**, then the team uses those capabilities.

## Ownership transfer
- Ownership transfer is a **team role change** inside the same workspace.
- It does not move data to a different workspace by itself.
