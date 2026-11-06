# Upgrading from 0.6.13 to 0.6.14

0.6.14 is a rolling patch upgrade. Traffic keeps running throughout, and you can roll binaries back to 0.6.13 with the schema patch left in place.

## Upgrade

1. Install the 0.6.14 CLI.
2. Check lane ownership. This is read-only and exits 2 while lanes need repair:

   ```bash
   awa --database-url "$AWA_DATABASE_URL" storage lane-sequences
   ```

3. Run `awa migrate` once, with the schema owner as the effective role, while workers keep running:

   ```bash
   PGOPTIONS='-c role=awa_owner' \
     awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
   ```

4. Read the output. The `definer_lane_provisioning` patch reports `applied`. If it could not reassign some lane sequences, `awa migrate` prints a warning; run `awa storage lane-sequences` for the statements to run and the role to run them as (see [Check and repair lane ownership](security.md#check-and-repair-lane-ownership)). The migration exit status does not depend on them.
5. Roll workers, producers and admin processes to 0.6.14, in any order.

## What `awa migrate` changes

The schema stays at v40. The `definer_lane_provisioning` patch installs `provision_lane` and `provision_lane_sequences`, two `SECURITY DEFINER` functions owned by the schema owner, in `awa` and every other queue-storage schema. It changes the three lane helpers to create a missing sequence through them, and adds the read-only `awa.lane_sequence_problems(schema)` and `awa.repair_lane_sequences(schema)`. Every new lane sequence then belongs to the schema owner and carries the privileges of the head table it backs, whichever role creates the lane: a producer enqueueing to a new queue, priority or shard, the maintenance leader promoting into a shard after `enqueue_shards` was raised, or `awa storage prepare-queue` run as any role.

The patch then repairs existing lane sequences. Each one not owned by the schema owner is reassigned if the applying role can `SET ROLE` to both its current owner and the schema owner. Missing grants are added if the applying role can act as the owner. A sequence it cannot change is reported and left as it was. The patch never fails because of one.

The patch:

- replaces functions and changes only the lane sequences that need it. `ALTER SEQUENCE ... OWNER` holds that sequence's lock until the patch commits, so enqueues and claims on those lanes wait for the commit. Correctly owned lanes are not touched;
- keeps every lane's sequence position;
- leaves storage routing unchanged;
- applies as one transaction, so an interrupted run leaves nothing applied; run it again.

0.6.13 and earlier binaries call the same helpers, so lanes they create after the patch are owned by the schema owner too. A 0.6.14 binary against a database without the patch detects that `provision_lane` is missing and creates lanes as 0.6.13 did.

### External migration runners

`awa migrate --sql --pending` prints the `R__definer_lane_provisioning` patch on a 0.6.13 database; with `--extract-to`, apply `R__definer_lane_provisioning.sql` as a repeatable script after `V40`. Run it as the schema owner, then run `awa storage lane-sequences` to check for leftovers.

## Roll back

Roll binaries back to 0.6.13 at any time. Leave the patch in place: 0.6.13 calls the same helpers with the same arguments.
