// Shared by the physical probe and the SQL block-size regression model.
// PostgreSQL 17/18 FSM_TREE_DEPTH uses four levels below 4096-byte BLCKSZ,
// three otherwise. The initial leaf allocation therefore spans that many pages.
// https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/storage/freespace/freespace.c
// https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/storage/freespace/freespace.c
// `block_size` is supplied by the enclosing SQL query.
macro_rules! brin_fsm_empty_pages_sql {
    () => {
        "CASE WHEN block_size < 4096 THEN 4 ELSE 3 END"
    };
}
pub(crate) use brin_fsm_empty_pages_sql;
