from enum import Enum
from typing import (
    ClassVar,
    Optional,
)

from pydantic import (
    BaseModel,
    validator,
)

from .util import none_to_zero


class OehValidationError(str, Enum):
    MISSING = "missing"
    TOO_SHORT = "too_short"
    TOO_FEW = "too_few"
    LACKS_CLARITY = "lacks_clarity"
    INVALID_SPELLING = "invalid_spelling"

    _lut: ClassVar[dict]


OehValidationError._lut = {
    e.value: e for _, e in OehValidationError.__members__.items()
}


class MaterialFieldValidation(BaseModel):
    missing: Optional[int]
    too_short: Optional[int]
    too_few: Optional[int]
    lacks_clarity: Optional[int]
    invalid_spelling: Optional[int]

    # validators
    _none_to_zero = validator("*", pre=True, allow_reuse=True)(none_to_zero)
