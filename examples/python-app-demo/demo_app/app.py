"""FastAPI app showing transactional enqueue with Awa."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.ext.asyncio import async_sessionmaker

from .shared import (
    create_app_engine,
    create_checkout,
    create_client,
    ensure_app_schema,
    list_recent_orders,
)


class CheckoutRequest(BaseModel):
    customer_email: EmailStr
    total_cents: int = Field(gt=0)
    checkout_id: str | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = create_client()
    try:
        await client.migrate()
    finally:
        await client.close()
    db = create_app_engine()
    try:
        # Any failure between engine creation and `yield` must still
        # dispose() the pool — including ensure_app_schema(), which
        # acquires a connection and runs DDL.
        await ensure_app_schema(db)
        app.state.db = db
        app.state.sessions = async_sessionmaker(db, expire_on_commit=False)
        yield
    finally:
        await db.dispose()


app = FastAPI(
    title="Awa Demo Shop",
    summary="Tiny FastAPI app using transactional enqueue with Awa",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/orders")
async def orders() -> list[dict[str, object]]:
    async with app.state.sessions() as session:
        return await list_recent_orders(session, limit=20)


@app.post("/orders/checkout")
async def checkout(request: CheckoutRequest) -> dict[str, object]:
    async with app.state.sessions() as session:
        return await create_checkout(
            session,
            customer_email=request.customer_email,
            total_cents=request.total_cents,
            order_id=request.checkout_id,
        )
