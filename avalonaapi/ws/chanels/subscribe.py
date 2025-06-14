from avalonaapi.ws.chanels.base import Base
import datetime
import avalonaapi.constants as OP_code


class Subscribe(Base):
    name = "subscribeMessage"

    def __call__(self, active_id, size):
        data = {"name": "candle-generated",
                "params": {
                    "routingFilters": {
                        "active_id": str(active_id),
                        "size": int(size)
                    }
                }
                }
        self.send_websocket_request(self.name, data)


class Subscribe_candles(Base):
    name = "subscribeMessage"

    def __call__(self, active_id):

        data = {"name": "candles-generated",
                "params": {
                    "routingFilters": {
                        "active_id": str(active_id)
                    }
                }
                }

        self.send_websocket_request(self.name, data)


class Subscribe_Instrument_Quites_Generated(Base):
    name = "subscribeMessage"

    def __call__(self, ACTIVE, expiration_period):
        data = {
            "name": "instrument-quotes-generated",
            "params": {
                "routingFilters": {
                    "active": int(OP_code.ACTIVES[ACTIVE]),
                    "expiration_period": int(expiration_period*60),
                    "kind": "digital-option",

                },
            },
            "version": "1.0"
        }
        self.send_websocket_request(self.name, data)

    def get_digital_expiration_time(self, duration):
        exp = int(self.api.timesync.server_timestamp)
        value = datetime.datetime.fromtimestamp(exp)
        minute = int(value.strftime('%M'))
        # second=int(value.strftime('%S'))
        ans = exp-exp % 60  # delete second
        ans = ans+(duration-minute % duration)*60
        if exp > ans-10:
            ans = ans+(duration)*60

        return ans


class Subscribe_top_assets_updated(Base):
    name = "subscribeMessage"

    def __call__(self, instrument_type):

        data = {"name": "top-assets-updated",
                "params": {
                    "routingFilters": {
                        "instrument_type": str(instrument_type)

                    }
                },
                "version": "1.2"
                }
        self.send_websocket_request(self.name, data)


class Subscribe_commission_changed(Base):
    name = "subscribeMessage"
    def __call__(self, instrument_type):

        data = {"name": "commission-changed",
                "params": {
                    "routingFilters": {
                        "instrument_type": str(instrument_type)
                    }
                },
                "version": "1.0"
                }
        self.send_websocket_request(self.name, data)


class Subscribe_live_deal(Base):
    name = "subscribeMessage"

    def __call__(self, name, active_id, _type):
     # "live-deal-binary-option-placed"
     # "live-deal-digital-option"
        if name == "live-deal-binary-option-placed":
            _type_name = "option_type"  # turbo/binary
            _active_id = "active_id"
        elif name == "live-deal-digital-option":
            _type_name = "expiration_type"
            _active_id = "instrument_active_id"
        elif name == "live-deal":
            _type_name = "instrument_type"
            _active_id = "instrument_active_id"

        data = {"name": name,
                "params": {
                    "routingFilters": {
                        _active_id: int(active_id),
                        _type_name: str(_type)
                    }
                },
                "version": "2.0"
                }
        self.send_websocket_request(self.name, data)


class SubscribeDigitalPriceSplitter(Base):
    name = "subscribeMessage"

    def __call__(self, asset_id):
        data = {
            "name": "price-splitter.client-price-generated",
            "version": "1.0",
            "params": {
                "routingFilters": {
                    "instrument_type": "digital-option",
                    "asset_id": int(asset_id)
                }
            }
        }

        self.send_websocket_request(self.name, msg=data)
