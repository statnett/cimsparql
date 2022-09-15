from enum import auto

from strenum import StrEnum


class CimStrEnum(StrEnum):
    @classmethod
    def pred(cls) -> str:
        return f"cim:{cls.__name__}"


class Impedance(StrEnum):
    r = auto()
    x = auto()


class GeneratorTypes(StrEnum):
    Hydro = auto()
    Thermal = auto()
    Wind = auto()


class IdentifiedObject(CimStrEnum):
    name = "cim:IdentifiedObject.name"
    mRID = "cim:IdentifiedObject.mRID"


class ConverterTypes(CimStrEnum):
    VoltageSourceConverter = "ALG:VoltageSourceConverter"
    DCConverter = "ALG:DCConverter"
    VsConverter = "cim:VsConverter"
    CsConverter = "cim:CsConverter"
    DCConverterUnit = "cim:DCConvertUnit"


class LoadTypes(CimStrEnum):
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


class SvStatus(CimStrEnum):
    inService = "cim:SvStatus.inService"
    ConductingEquipment = "cim:SvStatus.ConductingEquipment"


class SvTapStep(CimStrEnum):
    TapChanger = "cim:SvTapStep.TapChanger"
    position = "cim:SvTapStep.position"


class SvPowerFlow(CimStrEnum):
    Terminal = "cim:SvPowerFlow.Terminal"
    p = "cim:SvPowerFlow.p"
    q = "cim:SvPowerFlow.q"


class SvVoltage(CimStrEnum):
    TopologicalNode = "cim:SvVoltage.TopologicalNode"
    v = "cim:SvVoltage.v"
    angle = "cim:SvVoltage.angle"


class GeneratingUnit(CimStrEnum):
    marketCode = "SN:GeneratingUnit.marketCode"
    maxOperatingP = "cim:GeneratingUnit.maxOperatingP"
    minOperatingP = "cim:GeneratingUnit.minOperatingP"
    groupAllocationMax = "SN:GeneratingUnit.groupAllocationMax"
    groupAllocationWeight = "SN:GeneratingUnit.groupAllocationWeight"
    ScheduleResource = "SN:GeneratingUnit.ScheduleResource"


class SynchronousMachine(CimStrEnum):
    GeneratingUnit = "cim:SynchronousMachine.GeneratingUnit"


class WindGeneratingUnit(CimStrEnum):
    WindPowerPlant = "SN:WindGeneratingUnit.WindPowerPlant"


class VoltageLevel(CimStrEnum):
    BaseVoltage = "cim:VoltageLevel.BaseVoltage"
    Substation = "cim:VoltageLevel.Substation"


class BaseVoltage(CimStrEnum):
    nominalVoltage = "cim:BaseVoltage.nominalVoltage"


class TransformerEnd(CimStrEnum):
    Terminal = "cim:TransformerEnd.Terminal"
    endNumber = "cim:TransformerEnd.endNumber"


class PowerTransformerEnd(CimStrEnum):
    PowerTransformer = "cim:PowerTransformerEnd.PowerTransformer"
    phaseAngleClock = "cim:PowerTransformerEnd.phaseAngleClock"
