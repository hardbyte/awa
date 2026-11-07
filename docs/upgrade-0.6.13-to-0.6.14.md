# Upgrading from 0.6.13 to 0.6.14

0.6.14 is a rolling patch upgrade. Traffic keeps running throughout, and you can roll binaries back to 0.6.13 with the schema patch left in place.

## Upgrade

1. Install the 0.6.14 CLI.
2. Run `awa migrate` once, with the schema owner as the effective role, while workers keep running:

   ```bash
   PGOPTIONS='-c role=awa_owner' \
     awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
   ```

3. Check lane ownership after the patch installs its diagnostic helper. This is read-only and exits 2 while lanes need repair:

   ```bash
   awa --database-url "$AWA_DATABASE_URL" storage lane-sequences
   ```

4. Read the output. The `definer_lane_provisioning` patch reports `applied`, followed by how many lane sequences were repaired. Any left over are listed in warnings: a busy one needs another run of `awa storage lane-sequences --repair` as the migration role (without `PGOPTIONS`), and the others name the roles that can repair them (see [Check and repair lane ownership](security.md#check-and-repair-lane-ownership)). The migration exit status does not depend on them.
5. Roll workers, producers and admin processes to 0.6.14, in any order.

## What `awa migrate` changes

The schema stays at v40. The `definer_lane_provisioning` patch installs `provision_lane` and `provision_lane_sequences`, two `SECURITY DEFINER` functions owned by the schema owner, in `awa` and every other queue-storage schema. It changes the three lane helpers to create a missing sequence through them, and adds the read-only `awa.lane_sequence_problems(schema)` and `awa.repair_lane_sequence(schema, sequence)`. Every new lane sequence then belongs to the schema owner and carries the privileges of the head table it backs, whichever role creates the lane: a producer enqueueing to a new queue, priority or shard, the maintenance leader promoting into a shard after `enqueue_shards` was raised, or `awa storage prepare-queue` run as any role.

Once the patch commits, `awa migrate` repairs existing lane sequences, each in its own short transaction. A sequence not owned by the schema owner is reassigned if the role holds its current owner's privileges and can `SET ROLE` to the schema owner. Missing grants are added if the role holds the owner's privileges. Each sequence is tried as the effective role and then as the login role. Reassigning waits at most 300 ms for the sequence's lock, which an open enqueue or claim on that lane holds, and holds it only for that sequence's transaction. A sequence that is busy, or that neither role may change, is reported and left as it was.

If an earlier build already applied the patch, run `awa migrate` again with the updated CLI. It detects the older installed repair helper and reapplies the repeatable patch before attempting repairs.

The patch:

- only replaces functions, gives the two provisioning functions to the schema owner and grants `EXECUTE` on them to `PUBLIC`; it takes no sequence or table locks;
- keeps every lane's sequence position;
- leaves storage routing unchanged;
- applies as one transaction, so an interrupted run leaves nothing applied; run it again.

0.6.13 and earlier binaries call the same helpers, so lanes they create after the patch are owned by the schema owner too. A 0.6.14 binary against a database without the patch detects that `provision_lane` is missing and creates lanes as 0.6.13 did.

### External migration runners

`awa migrate --sql --pending` prints the `R__definer_lane_provisioning` patch on a 0.6.13 database; with `--extract-to`, apply `R__definer_lane_provisioning.sql` as a repeatable script after `V40`. Run it as the role that owns the existing `awa` functions, normally the schema owner; `CREATE OR REPLACE` fails with `must be owner of function` otherwise, and nothing is applied. The script does not repair existing lane sequences, so then run `awa storage lane-sequences --repair` as the migration role itself.

Each `R__` patch carries the full lane-helper definitions, so an older patch file applied after this one reinstalls the 0.6.13 helpers:

- Replace every `R__` file with the set the 0.6.14 CLI extracts, together, rather than adding `R__definer_lane_provisioning.sql` beside older ones.
- Don't roll the runner's SQL files back to 0.6.13 when rolling binaries back. Leave the patch in place, as above.
- Keep the `V` files exactly as originally applied. Extract with `--pending`; a full `--extract-to` rewrites `V23`, which fails a checksum-validating runner.

## Roll back

Roll binaries back to 0.6.13 at any time. Leave the patch in place: 0.6.13 calls the same helpers with the same arguments.
