from datetime import timedelta, datetime

from generator import *
import random
from time import sleep

sleep_time = 1

TRANSACTION_CACHE = 0


def _generate_config(charge_point_id: str, base_time: datetime) -> Dict:
    return {
        "charge_point_id": charge_point_id,
        "connector_id": random.randint(1, 5),
        "rfid": str(random.getrandbits(64)),
        "base_time": base_time
    }


def _decorate(charge_point_id: str, body: Dict) -> Dict:
    now = datetime.now(tz=pytz.utc)
    data = {
        "charge_point_id": charge_point_id,
        "write_timestamp": now.isoformat(),
        "body": body
    }
    print(data)
    return data


def create_transaction(transaction_cache: int, connector_id: int, rfid: str):
    transactions_start = [
        create_start_transaction(connector_id=connector_id, rfid=rfid)
    ]

    transaction_cache += 1

    charge_duration = random.randint(3, 25)
    current_meter_values = None
    transaction_ongoing = [create_meter_values(transaction_id=TRANSACTION_CACHE, connector_id=x["connector_id"],
                                               current_meter_values=current_meter_values) for i in
                           range(charge_duration)]

    last_meter_reading = list(filter(lambda x: x["measurand"] in [Measurand.energy_active_import_register],
                                     transaction_ongoing[-1]["meter_value"][0]["sampled_value"]))

    transaction_stop = [
        create_stop_transaction(transaction_id=transaction_cache, rfid=x["rfid"],
                                meter_stop_wh=last_meter_reading[0]["value"])
    ]

    return transactions_start + transaction_ongoing + transaction_stop


charge_point_ids = [
    {
        "charge_point_id": "AL1000",
        "base_time": datetime(2022, 10, 2, 15, 16, 17, 345, tzinfo=pytz.utc)
    }
]


def time_tick_value(tick: int):
    return timedelta(seconds=120*tick)

for x in list(map(lambda x: _generate_config(**x), charge_point_ids)):
    current_time = x["base_time"]

    base = [
        create_boot_notification(charge_point_id=x["charge_point_id"]),
        create_heartbeat(),
        create_heartbeat(),
        create_heartbeat(),
        create_heartbeat(),
        create_heartbeat(),
    ]

    transactions = create_transaction(transaction_cache=TRANSACTION_CACHE)
    TRANSACTION_CACHE += 1


    # # result = [ f({"charge_point_id": x["charge_point_id"], "timestamp": current_time+time_tick_value(index)}) for index, f in base ]
    # result = [ f(**{"charge_point_id": x["charge_point_id"], "rfid": x["rfid"], "connector_id": x["connector_id"], "timestamp": current_time+time_tick_value(idx)}) for idx, f in enumerate(base) ]
    # print(result)


    #
    # result = base + transactions
    # print(result)











# for x in list(map(lambda x: _generate_config(**x), charge_point_ids)):
#     current_time = x["base_time"]
#     _decorate(charge_point_id=x["charge_point_id"], body=create_boot_notification(charge_point_id=x["charge_point_id"]))
#     for i in range(5):
#         sleep(sleep_time)
#         _decorate(charge_point_id=x["charge_point_id"], body=create_heartbeat())
#
#     sleep(sleep_time)
#     _decorate(charge_point_id=x["charge_point_id"], body=create_start_transaction(connector_id=x["connector_id"], rfid=x["rfid"]))
#
#     transaction_cache += 1
#     transaction_id = transaction_cache
#
#     charge_duration = random.randint(3, 25)
#     current_meter_values = None
#     for i in range(charge_duration):
#         sleep(sleep_time)
#         current_meter_values = create_meter_values(transaction_id=transaction_id, connector_id=x["connector_id"], current_meter_values=current_meter_values)
#         _decorate(charge_point_id=x["charge_point_id"], body=current_meter_values)
