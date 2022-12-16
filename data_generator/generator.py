import json
import random
from datetime import datetime
from typing import Dict, Optional, List, Callable

import pytz
from ocpp.v16 import call
from ocpp.v16.datatypes import SampledValue, MeterValue
from ocpp.v16.enums import ReadingContext, Location, ValueFormat, Phase, Measurand, UnitOfMeasure


def create_heartbeat(**kwargs) -> (Dict, str, Callable):
    return call.HeartbeatPayload().__dict__, "Heartbeat", lambda x: {}


def create_boot_notification(charge_point_id: str, **kwargs) -> (Dict, str, Callable):
    return call.BootNotificationPayload(
        charge_point_model=f"BB-{random.randint(0,5)}",
        charge_point_vendor="ChargeAwesome LLC",
        charge_point_serial_number=charge_point_id
    ).__dict__, "BootNotification", lambda x: {}


def create_start_transaction(connector_id: int, rfid: str, **kwargs) -> (Dict, str, Callable):
    return call.StartTransactionPayload(
        connector_id=connector_id,
        id_tag=rfid,
        meter_start=random.randint(1,60),
        timestamp=datetime.now(tz=pytz.utc).isoformat()
    ).__dict__, "StartTransaction", lambda x: {"timestamp": x}


def create_stop_transaction(transaction_id: int, rfid: str, meter_stop_wh: int, **kwargs) -> (Dict, str, Callable):
    return call.StopTransactionPayload(
        transaction_id=transaction_id,
        id_tag=rfid,
        meter_stop=meter_stop_wh,
        timestamp=datetime.now(tz=pytz.utc).isoformat()
    ).__dict__, "StopTransaction", lambda x: {"timestamp": x}


def _add_noise(base: float) -> str:
    diff = random.uniform(0.10, 0.25)
    if base > 0:
        return "{:0.2f}".format(random.uniform(float(base) * (1.0 - diff), float(base) * (1.0 + diff)))
    else:
        return "{:0.2f}".format(random.uniform(float(base), float(base) * (1.0 + diff)))


def _add_ceiling_noise_to(base: float) -> str:
    diff = random.uniform(0.05, 0.08)
    value = base + random.uniform((float(base) + 0.1), (float(base) + 0.1) * (1.0 + diff))
    return "{:0.2f}".format(value)


def create_meter_values(transaction_id: int, connector_id: int, current_meter_values: Optional[Dict], **kwargs) -> (Dict, str, Callable):
    if current_meter_values is not None:
        sample_value = list(filter(lambda x: x["measurand"] in [Measurand.energy_active_import_register], current_meter_values["meter_value"][0]["sampled_value"]))
        prev_total_charge = float(sample_value[0]["value"])
    else:
        prev_total_charge = float(0)

    sampled_values = [
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.voltage,
            phase=Phase.l1_n,
            unit=UnitOfMeasure.v,
            value=_add_noise(0.0),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.current_import,
            phase=Phase.l1,
            unit=UnitOfMeasure.a,
            value=_add_noise(13.5),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.power_active_import,
            phase=Phase.l1,
            unit=UnitOfMeasure.w,
            value=_add_noise(3335.0),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.voltage,
            phase=Phase.l2_n,
            unit=UnitOfMeasure.v,
            value=_add_noise(229.3),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.current_import,
            phase=Phase.l2,
            unit=UnitOfMeasure.a,
            value=_add_noise(14.34),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.power_active_import,
            phase=Phase.l2,
            unit=UnitOfMeasure.w,
            value=_add_noise(3468.3),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.voltage,
            phase=Phase.l3_n,
            unit=UnitOfMeasure.v,
            value=_add_noise(224.3),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.current_import,
            phase=Phase.l3,
            unit=UnitOfMeasure.a,
            value=_add_noise(14.53),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.power_active_import,
            phase=Phase.l3,
            unit=UnitOfMeasure.w,
            value=_add_noise(3465.0),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.voltage,
            unit=UnitOfMeasure.wh,
            value=_add_noise(224.3),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.voltage,
            phase=Phase.l1_n,
            unit=UnitOfMeasure.v,
            value=_add_noise(14.53),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.current_import,
            phase=Phase.l1,
            unit=UnitOfMeasure.a,
            value=_add_noise(3465.0),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.power_active_import,
            phase=Phase.l1,
            unit=UnitOfMeasure.w,
            value=_add_noise(7058.0),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.voltage,
            phase=Phase.l2_n,
            unit=UnitOfMeasure.v,
            value=_add_noise(0.0),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.current_import,
            phase=Phase.l2,
            unit=UnitOfMeasure.a,
            value=_add_noise(4.02),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.power_active_import,
            phase=Phase.l2,
            unit=UnitOfMeasure.w,
            value=_add_noise(879.0),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.voltage,
            phase=Phase.l3_n,
            unit=UnitOfMeasure.v,
            value=_add_noise(224.5),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.current_import,
            phase=Phase.l3,
            unit=UnitOfMeasure.a,
            value=_add_noise(3.88),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.power_active_import,
            phase=Phase.l3,
            unit=UnitOfMeasure.w,
            value=_add_noise(887.0),
        ),
        SampledValue(
            context=ReadingContext.sample_periodic,
            format=ValueFormat.raw,
            location=Location.outlet,
            measurand=Measurand.energy_active_import_register,
            unit=UnitOfMeasure.wh,
            value=_add_ceiling_noise_to(prev_total_charge)
        ),
    ]

    meter_value = MeterValue(
        timestamp=str(datetime.now(tz=pytz.utc).isoformat()),
        sampled_value=[json.loads(json.dumps(v.__dict__)) for v in sampled_values]
    )

    meter_values = call.MeterValuesPayload(
        connector_id=connector_id,
        transaction_id=transaction_id,
        meter_value=[json.loads(json.dumps(meter_value.__dict__))]
    )


    return meter_values.__dict__, "MeterValues", lambda x: {"meter_value": [{"timestamp": x} | {"sampled_value": meter_value.__dict__["sampled_value"]}]}

