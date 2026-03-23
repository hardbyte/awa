"""FastAPI app showing transactional enqueue with Awa."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel, EmailStr, Field

from .shared import create_checkout, create_client, ensure_app_schema, list_recent_orders


class CheckoutRequest(BaseModel):
    customer_email: EmailStr
    total_cents: int = Field(gt=0)
    checkout_id: str | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = create_client()
    await client.migrate()
    await ensure_app_schema(client)
    app.state.client = client
    yield


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
    return await list_recent_orders(app.state.client, limit=20)


@app.post("/orders/checkout")
async def checkout(request: CheckoutRequest) -> dict[str, object]:
    return await create_checkout(
        app.state.client,
        customer_email=request.customer_email,
        total_cents=request.total_cents,
        order_id=request.checkout_id,
    )
