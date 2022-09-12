from enum import auto

from strenum import StrEnum


class Impedance(StrEnum):
    r = auto()
    x = auto()


class GeneratorTypes(StrEnum):
    Hydro = auto()
    Thermal = auto()
    Wind = auto()


class ConverterTypes(StrEnum):
    VoltageSourceConverter = "ALG:VoltageSourceConverter"
    DCConverter = "ALG:DCConverter"
    VsConverter = "cim:VsConverter"
    CsConverter = "cim:CsConverter"
    DCConverterUnit = "cim:DCConvertUnit"


class LoadTypes(StrEnum):
    ConformLoad = "cim:ConformLoad"
    NonConformLoad = "cim:NonConformLoad"
    EnergyConsumer = "cim:EnergyConsumer"


class Power(StrEnum):
    p = auto()
    q = auto()


class Voltage(StrEnum):
    v = auto()
    angle = auto()


class Rates(StrEnum):
    Normal = auto()
    Warning = auto()
    Overload = auto()


class TapChangerObjects(StrEnum):
    high = auto()
    low = auto()
    neutral = auto()


class SyncVars(StrEnum):
    sn = auto()
    p = auto()
    q = auto()
