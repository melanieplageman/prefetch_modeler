from enum import Enum, auto
from fractions import Fraction
from typing import Optional
from dataclasses import dataclass
import math

class Unit(Enum):
    MICROSECOND = auto()
    MILLISECOND = auto()
    SECOND = auto()

class Duration:
    def __init__(self, microseconds=0, milliseconds=0, seconds=0):
        self.total = microseconds + (milliseconds * 1000) + (seconds * 1000 * 1000)

    # TODO: make this do per unit and get rid of post_init in workload so this
    # looks correct in output
    def __str__(self):
        return f'{self.total} microseconds'

class BaseRate:
    def __init__(self):
        self.value = None

class InfiniteRate(BaseRate):
    def __init__(self):
        self.value = math.inf

    def __str__(self):
        return 'Infinity'

class Rate(BaseRate):
    def __init__(self, per_microsecond=0, per_millisecond=0, per_second=0):
        if per_microsecond:
            if per_millisecond or per_second:
                raise ValueError('Can only specify one Rate unit')
            self.value = Fraction(per_microsecond)
            self.original_unit = Unit.MICROSECOND

        elif per_millisecond:
            if per_microsecond or per_second:
                raise ValueError('Can only specify one Rate unit')
            self.value = Fraction(per_millisecond, 1000)
            self.original_unit = Unit.MILLISECOND

            if self.value.denominator != 1 and self.value.numerator != 1:
                raise ValueError(f"per_millisecond={per_millisecond} must be divisible by 1000")

        elif per_second:
            if per_millisecond or per_microsecond:
                raise ValueError('Can only specify one Rate unit')
            self.value = Fraction(per_second, 1000 * 1000)
            self.original_unit = Unit.SECOND

            if self.value.denominator != 1 and self.value.numerator != 1:
                raise ValueError(f"per_second={per_second} must be divisible by 1,000,000")

    def __str__(self):
        extension = f' per {(self.original_unit.name).lower()}'

        if self.original_unit == Unit.MICROSECOND:
            return f'{int(self.value)}' + extension
        if self.original_unit == Unit.MILLISECOND:
            return f'{int(self.value * 1000)}' + extension
        if self.original_unit == Unit.SECOND:
            return f'{int(self.value * 1000 * 1000)}' + extension


@dataclass(kw_only=True)
class Interval:
    tick: int
    rate: Fraction
    length: Optional[int] = None

