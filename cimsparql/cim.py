# Miscellaneous cim classes

from strenum import StrEnum


class LineTypes(StrEnum):
    ACLineSegment = "cim:ACLineSegment"
    SeriesCompensator = "cim:SeriesCompensator"


BIDDINGAREA = "SN:MarketDeliveryPoint.BiddingArea"
CNODE_CONTAINER = "cim:ConnectivityNode.ConnectivityNodeContainer"
DELIVERYPOINT = "SN:Substation.MarketDeliveryPoint"
EQUIP_CONTAINER = "cim:Equipment.EquipmentContainer"
GEO_REG = "cim:SubGeographicalRegion"
ID_OBJ = "cim:IdentifiedObject"
MARKETCODE = "SN:BiddingArea.marketCode"
OPERATIONAL_LIMIT_SET = "cim:OperationalLimit.OperationalLimitSet"
SUBSTATION = "cim:VoltageLevel.Substation"
SYNC_MACH = "cim:SynchronousMachine"
TC_EQUIPMENT = "cim:Terminal.ConductingEquipment"
T_SEQUENCE = "cim:Terminal.sequenceNumber"
TC_NODE = "cim:Terminal.ConnectivityNode"
TR_WINDING = "cim:PowerTransformerEnd"
TR_END = "cim:TransformerEnd"
TN = "cim:TopologicalNode"
