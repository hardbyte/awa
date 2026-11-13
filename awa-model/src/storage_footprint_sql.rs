// Pages an empty BRIN index keeps in its free space map: PostgreSQL 17/18
// FSM_TREE_DEPTH is four levels below a 4096-byte block size, three otherwise.
// https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/storage/freespace/freespace.c
// https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/storage/freespace/freespace.c
// `block_size` is supplied by the enclosing SQL query.
macro_rules! brin_fsm_empty_pages_sql {
    () => {
        "CASE WHEN block_size < 4096 THEN 4 ELSE 3 END"
    };
}
pub(crate) use brin_fsm_empty_pages_sql;
