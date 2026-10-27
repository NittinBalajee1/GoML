"""Strict input contracts and record-level validation."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator
from pydantic import ValidationError


class Entity(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    entity_id: UUID
    first_name: str = Field(min_length=1)
    last_name: str = Field(min_length=1)
    contact_email: EmailStr
    registration_date: date | datetime
    region: str = Field(min_length=1)


class Transaction(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    transaction_id: UUID
    entity_id: UUID
    category: str = Field(min_length=1)
    transaction_amount: Decimal = Field(gt=0, allow_inf_nan=False)
    currency: Literal["USD", "EUR", "GBP"]
    timestamp: datetime

    @field_validator("transaction_amount", mode="before")
    @classmethod
    def reject_boolean_amount(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("transaction_amount must be numeric, not boolean")
        return value


def validate_records(
    model: type[BaseModel], records: list[dict[str, Any]]
) -> tuple[list[BaseModel], list[dict[str, Any]]]:
    valid: list[BaseModel] = []
    invalid: list[dict[str, Any]] = []
    for record in records:
        try:
            valid.append(model.model_validate(record))
        except ValidationError as exc:
            invalid.append({"record": record, "errors": exc.errors(include_url=False)})
    return valid, invalid
