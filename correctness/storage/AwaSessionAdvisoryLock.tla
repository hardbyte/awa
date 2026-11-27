--------------------------- MODULE AwaSessionAdvisoryLock ---------------------------
(*
 * Session advisory locks held on pooled connections.
 *
 * A session advisory lock (pg_advisory_lock, pg_try_advisory_lock) outlives
 * the statement and the transaction that took it: only pg_advisory_unlock or
 * the end of the session releases it. Awa's connections come from a pool, so a
 * connection that goes back to the pool still holding such a lock keeps it
 * until the pool closes that connection, and every other session that wants
 * the lock waits. This model checks the two places awa takes these locks.
 *
 * 1. PL/pgSQL helpers (set_sequence_next, the enqueue-head trigger's block
 *    allocation, enqueue_ring_cursor on 0.7). The statement takes the lock,
 *    does its work, and unlocks. Any step can raise: an ordinary error, or a
 *    cancel or statement timeout, both of which raise query_canceled. Postgres
 *    checks for interrupts right after granting an advisory lock, so a cancel
 *    can be raised with the lock just granted. An EXCEPTION handler runs only
 *    for an error raised inside its block that matches one of its conditions;
 *    WHEN OTHERS does not match query_canceled. A handler that runs and
 *    unlocks releases the lock; any other way out leaves it held when the
 *    statement ends and the connection returns to the pool.
 *
 * 2. Rust holders (the maintenance leader, the batch-operation runner, the 0.6
 *    migration runner). The holder checks a connection out of the pool, takes
 *    the lock, works, unlocks, and returns the connection. Its future can be
 *    dropped at any await point, and the unlock itself can fail. A plain
 *    PoolConnection goes back to the pool when dropped. The SessionAdvisoryLock
 *    guard returns it only after a successful unlock and closes it otherwise,
 *    which ends the session and releases the lock.
 *
 * Constants select the code:
 *   LockInsideBlock      the lock is taken inside the block whose handler
 *                        unlocks (the fix), rather than before it.
 *   HandlerCatchesOthers the handler names OTHERS.
 *   HandlerCatchesCancel the handler names query_canceled.
 *   GuardClosesOnDrop    Rust holders use SessionAdvisoryLock.
 *
 * Abstractions: one lock key; lock counts are re-entrant per session, as in
 * Postgres; a handler that runs checks pg_locks before unlocking, so it
 * unlocks only a lock its session holds. Opening a replacement for a closed
 * connection is a fresh session holding nothing. Transaction-scoped locks,
 * statement work other than taking and releasing the lock, and which session
 * Postgres grants a contended lock to are not modelled.
 *)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Workers,
    Conns,
    LockInsideBlock,
    HandlerCatchesOthers,
    HandlerCatchesCancel,
    GuardClosesOnDrop,
    MaxRounds

ASSUME LockInsideBlock \in BOOLEAN
ASSUME HandlerCatchesOthers \in BOOLEAN
ASSUME HandlerCatchesCancel \in BOOLEAN
ASSUME GuardClosesOnDrop \in BOOLEAN
ASSUME MaxRounds \in Nat

NoConn == "none"

VARIABLES
    connState,  \* conn -> "pooled" | "busy" | "closed"
    held,       \* conn -> lock count held by the conn's session
    pc,         \* worker -> program counter
    conn,       \* worker -> conn checked out, or NoConn
    inBlock,    \* worker -> whether the PL/pgSQL statement is inside its block
    rounds      \* worker -> statements or holder sections started

vars == <<connState, held, pc, conn, inBlock, rounds>>

PcValues == {
    "idle",
    \* PL/pgSQL helper
    "p_request",   \* waiting for or taking the lock
    "p_granted",   \* lock just granted; interrupts are checked here
    "p_enter",     \* lock taken before the block: entering it
    "p_body",      \* the helper's work
    "p_exit",      \* statement ended; connection goes back to the pool
    \* Rust holder
    "r_lock",      \* pg_try_advisory_lock in flight
    "r_work",      \* holding the lock
    "r_unlock"     \* pg_advisory_unlock in flight
}

TypeOK ==
    /\ connState \in [Conns -> {"pooled", "busy", "closed"}]
    /\ held \in [Conns -> Nat]
    /\ pc \in [Workers -> PcValues]
    /\ conn \in [Workers -> Conns \cup {NoConn}]
    /\ inBlock \in [Workers -> BOOLEAN]
    /\ rounds \in [Workers -> 0..MaxRounds]

Init ==
    /\ connState = [c \in Conns |-> "pooled"]
    /\ held = [c \in Conns |-> 0]
    /\ pc = [w \in Workers |-> "idle"]
    /\ conn = [w \in Workers |-> NoConn]
    /\ inBlock = [w \in Workers |-> FALSE]
    /\ rounds = [w \in Workers |-> 0]

\* The lock is free for conn c when no other session holds it (re-entrant).
LockFreeFor(c) == \A d \in Conns \ {c} : held[d] = 0

-----------------------------------------------------------------------------
(* Checking a connection out of the pool and starting work on it. *)

StartPlpgsql(w, c) ==
    /\ pc[w] = "idle"
    /\ rounds[w] < MaxRounds
    /\ connState[c] = "pooled"
    /\ connState' = [connState EXCEPT ![c] = "busy"]
    /\ conn' = [conn EXCEPT ![w] = c]
    /\ pc' = [pc EXCEPT ![w] = "p_request"]
    \* With the fix the lock request is the block's first statement.
    /\ inBlock' = [inBlock EXCEPT ![w] = LockInsideBlock]
    /\ rounds' = [rounds EXCEPT ![w] = @ + 1]
    /\ UNCHANGED held

StartRust(w, c) ==
    /\ pc[w] = "idle"
    /\ rounds[w] < MaxRounds
    /\ connState[c] = "pooled"
    /\ connState' = [connState EXCEPT ![c] = "busy"]
    /\ conn' = [conn EXCEPT ![w] = c]
    /\ pc' = [pc EXCEPT ![w] = "r_lock"]
    /\ rounds' = [rounds EXCEPT ![w] = @ + 1]
    /\ UNCHANGED <<held, inBlock>>

\* The pool opens a fresh session in place of a closed connection.
Reopen(c) ==
    /\ connState[c] = "closed"
    /\ connState' = [connState EXCEPT ![c] = "pooled"]
    /\ held' = [held EXCEPT ![c] = 0]
    /\ UNCHANGED <<pc, conn, inBlock, rounds>>

-----------------------------------------------------------------------------
(* PL/pgSQL helper. *)

PGrant(w) ==
    LET c == conn[w] IN
    /\ pc[w] = "p_request"
    /\ LockFreeFor(c)
    /\ held' = [held EXCEPT ![c] = @ + 1]
    /\ pc' = [pc EXCEPT ![w] = "p_granted"]
    /\ UNCHANGED <<connState, conn, inBlock, rounds>>

\* No interrupt after the grant: carry on into the block (if the lock was
\* taken before it) or straight into the work.
PAfterGrant(w) ==
    /\ pc[w] = "p_granted"
    /\ pc' = [pc EXCEPT ![w] = IF inBlock[w] THEN "p_body" ELSE "p_enter"]
    /\ UNCHANGED <<connState, held, conn, inBlock, rounds>>

PEnterBlock(w) ==
    /\ pc[w] = "p_enter"
    /\ inBlock' = [inBlock EXCEPT ![w] = TRUE]
    /\ pc' = [pc EXCEPT ![w] = "p_body"]
    /\ UNCHANGED <<connState, held, conn, rounds>>

\* The work and the unlock succeed.
PSucceed(w) ==
    LET c == conn[w] IN
    /\ pc[w] = "p_body"
    /\ held' = [held EXCEPT ![c] = @ - 1]
    /\ pc' = [pc EXCEPT ![w] = "p_exit"]
    /\ inBlock' = [inBlock EXCEPT ![w] = FALSE]
    /\ UNCHANGED <<connState, conn, rounds>>

\* The handler matches when the error is raised inside the block and its
\* condition list names the error's condition.
HandlerRuns(w, kind) ==
    /\ inBlock[w]
    /\ \/ kind = "other" /\ HandlerCatchesOthers
       \/ kind = "cancel" /\ HandlerCatchesCancel

\* An error ends the statement. A matching handler unlocks the lock its
\* session holds, checked in pg_locks, then re-raises.
PRaise(w, kind) ==
    LET c == conn[w] IN
    /\ \/ pc[w] = "p_request" /\ kind = "cancel"  \* lock wait interrupted, not granted
       \/ pc[w] = "p_granted" /\ kind = "cancel"  \* interrupt checked just after the grant
       \/ pc[w] = "p_enter"   /\ kind = "cancel"
       \/ pc[w] = "p_body"
    /\ LET granted == pc[w] # "p_request" IN
       held' = IF granted /\ HandlerRuns(w, kind)
               THEN [held EXCEPT ![c] = @ - 1]
               ELSE held
    /\ pc' = [pc EXCEPT ![w] = "p_exit"]
    /\ inBlock' = [inBlock EXCEPT ![w] = FALSE]
    /\ UNCHANGED <<connState, conn, rounds>>

PExit(w) ==
    LET c == conn[w] IN
    /\ pc[w] = "p_exit"
    /\ connState' = [connState EXCEPT ![c] = "pooled"]
    /\ conn' = [conn EXCEPT ![w] = NoConn]
    /\ pc' = [pc EXCEPT ![w] = "idle"]
    /\ UNCHANGED <<held, inBlock, rounds>>

-----------------------------------------------------------------------------
(* Rust holder. *)

\* Hands the connection back: to the pool, or closed by the guard.
GiveBack(w, close) ==
    LET c == conn[w] IN
    /\ connState' = [connState EXCEPT ![c] = IF close THEN "closed" ELSE "pooled"]
    /\ held' = IF close THEN [held EXCEPT ![c] = 0] ELSE held
    /\ conn' = [conn EXCEPT ![w] = NoConn]
    /\ pc' = [pc EXCEPT ![w] = "idle"]

RTryLock(w) ==
    LET c == conn[w] IN
    /\ pc[w] = "r_lock"
    /\ IF LockFreeFor(c)
       THEN /\ held' = [held EXCEPT ![c] = @ + 1]
            /\ pc' = [pc EXCEPT ![w] = "r_work"]
            /\ UNCHANGED <<connState, conn>>
       ELSE GiveBack(w, FALSE)  \* not held: back to the pool either way
    /\ UNCHANGED <<inBlock, rounds>>

RDoneWorking(w) ==
    /\ pc[w] = "r_work"
    /\ pc' = [pc EXCEPT ![w] = "r_unlock"]
    /\ UNCHANGED <<connState, held, conn, inBlock, rounds>>

RUnlockOk(w) ==
    LET c == conn[w] IN
    /\ pc[w] = "r_unlock"
    /\ held' = [held EXCEPT ![c] = @ - 1]
    /\ connState' = [connState EXCEPT ![c] = "pooled"]
    /\ conn' = [conn EXCEPT ![w] = NoConn]
    /\ pc' = [pc EXCEPT ![w] = "idle"]
    /\ UNCHANGED <<inBlock, rounds>>

\* The unlock fails (a lost response, a statement timeout). The guard closes
\* the connection; a plain holder ignores the error and lets it go back.
RUnlockFails(w) ==
    /\ pc[w] = "r_unlock"
    /\ GiveBack(w, GuardClosesOnDrop)
    /\ UNCHANGED <<inBlock, rounds>>

\* The holder's future is dropped while the lock request, the work or the
\* unlock is in flight. A request in flight may already have been granted.
RDrop(w) ==
    LET c == conn[w] IN
    /\ pc[w] \in {"r_lock", "r_work", "r_unlock"}
    /\ \E grantedFirst \in BOOLEAN :
          LET took == pc[w] = "r_lock" /\ grantedFirst /\ LockFreeFor(c) IN
          IF GuardClosesOnDrop
          THEN GiveBack(w, TRUE)
          ELSE /\ connState' = [connState EXCEPT ![c] = "pooled"]
               /\ held' = IF took THEN [held EXCEPT ![c] = @ + 1] ELSE held
               /\ conn' = [conn EXCEPT ![w] = NoConn]
               /\ pc' = [pc EXCEPT ![w] = "idle"]
    /\ UNCHANGED <<inBlock, rounds>>

-----------------------------------------------------------------------------

\* Every worker has run its statements and holder sections. Any other state
\* without a step, such as a worker waiting for a lock no session will release,
\* is a deadlock TLC reports.
Finished ==
    /\ \A w \in Workers : pc[w] = "idle" /\ rounds[w] = MaxRounds
    /\ \A c \in Conns : connState[c] # "closed"
    /\ UNCHANGED vars

Next ==
    \/ Finished
    \/ \E w \in Workers, c \in Conns : StartPlpgsql(w, c) \/ StartRust(w, c)
    \/ \E c \in Conns : Reopen(c)
    \/ \E w \in Workers :
          \/ PGrant(w) \/ PAfterGrant(w) \/ PEnterBlock(w) \/ PSucceed(w)
          \/ PRaise(w, "other") \/ PRaise(w, "cancel") \/ PExit(w)
          \/ RTryLock(w) \/ RDoneWorking(w) \/ RUnlockOk(w)
          \/ RUnlockFails(w) \/ RDrop(w)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties. *)

\* A connection in the pool holds no session advisory lock, so the next
\* checkout neither inherits it nor blocks every other session's request.
NoPooledLock == \A c \in Conns : connState[c] = "pooled" => held[c] = 0

\* Mutual exclusion: at most one session holds the lock.
LockExclusive == \A c, d \in Conns : (held[c] > 0 /\ held[d] > 0) => c = d

\* A statement or holder section that has ended leaves no lock held by any
\* session that is not in use.
NoOrphanLock ==
    \A c \in Conns : held[c] > 0 => connState[c] = "busy"

=============================================================================
