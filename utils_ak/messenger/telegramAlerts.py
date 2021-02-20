import telegram
import enum


# todo: refactor


class EventType(enum.Enum):
    disconnected = "DISCONNECTED"
    connection_lost = "CONNECTION LOST"
    connection_failed = "CONNECTION FAILED"
    reconnected = "RECONNECTED"
    connected = "CONNECTED"
    snapshot_failed = "SNAPSHOT FAILED"
    gap = "GAP"


class NotifierBot:
    def __init__(self):
        # @qtb_bot token
        self.bot_token = "442059717:AAFUUCQ0mDcoOBYqTZrcoIa8JUY35o82b0E"

        # id of QTB private channel
        self.chat_id = "-1001364676826"

        self.bot = telegram.Bot(token=self.bot_token)

    def notify(self, exchange=None, time=None, errtype=None, curr=None, mss=None):
        if not mss:
            if errtype == EventType.disconnected:
                msg = "{} {}: a disconnect occured on {} at {}. \n Attempting reconnect...".format(
                    exchange.upper(), errtype.value, exchange, time
                )
            elif errtype == EventType.connected:
                msg = "{} {}: successfully connected to {} at {}.".format(
                    exchange.upper(), errtype.value, exchange, time
                )
            elif errtype == EventType.reconnected:
                msg = "{} {}: successfully reconnected to {} at {}.".format(
                    exchange.upper(), errtype.value, exchange, time
                )
            elif errtype == EventType.gap:
                msg = "{} {}: a gap of more than 1 minute has been observed when downloading from {} at {}".format(
                    exchange.upper(), errtype.value, exchange, time
                )
            elif errtype == EventType.connection_lost:
                msg = "{} {}: connection lost with {} at {}.".format(
                    exchange.upper(), errtype.value, exchange, time
                )
            elif errtype == EventType.connection_failed:
                msg = "{} {}: connection failed with {} at {}.".format(
                    exchange.upper(), errtype.value, exchange, time
                )
            elif errtype == EventType.snapshot_failed:
                msg = "{} {}: {} snapshot failed on {} at {}.".format(
                    exchange.upper(), errtype.value, curr, exchange, time
                )
            else:
                msg = "{}: UNKNOWN ERROR TYPE ENCOUNTRED ON {} AT {}!".format(
                    exchange.upper(), exchange, time
                )
        else:
            msg = mss

        self.bot.send_message(self.chat_id, msg)


# Test
if __name__ == "__main__":
    bt = NotifierBot()
    bt.notify("Binance", "time", EventType.reconnect)
