import paho.mqtt.client as mqtt
import threading
import json
import logging
import time
import os
from . import webull

logger = logging.getLogger(__name__)

class StreamConn :
    def __init__(self, debug_flg=False):
        self.onsub_lock = threading.RLock()
        self.oncon_lock = threading.RLock()
        self.onmsg_lock = threading.RLock()
        self.price_func = None
        self.order_func = None
        self.disconnect_func = None
        self.debug_flg = debug_flg
        self.total_volume={}
        self.client_order_upd = None
        self.client_streaming_quotes = None

    """
    ====Order status from platpush====
    topic _______: messageId, action, type, title, messageHeaders{popstatus, popvalidtime},
                      data {"tickerId, brokerId, secAccountId, orderId, filledQuantity, orderType,
                        orderStatus, messageProtocolVersion, messageProtocolUri,messageTitle, messageContent}

    ====Price updates from wspush====
    The topic of each message contains the message type and tickerId
    All price messages have a status field defined as F = pre-market, T = market hours, A = after-market
    Topic 102 is the most common streaming message used by the trading app,
        at times 102 for some crazy reason may not show a close price or volume, so it's not useful for a trading app
    High/low/open/close/volume are usually totals for the day
    Most message have these common fields: transId, pubId, tickerId, tradeStamp, trdSeq, status
    topic 101:  T: close, change, marketValue, changeRatio
    topic 102:  F/A: pPrice,  pChange, pChRatio,
                T: high(optional), low(optional), open(optional), close(optional), volume(optional),
                    vibrateRatio, turnoverRate(optional), change, changeRatio, marketValue
    topic 103:  F/A: deal:{trdBs(always N), volume, tradeTime(H:M:S), price}
                T: deal:{trdBs, volume, tradeTime(H:M:S), price}
    topic 104:  F/T/A: askList:[{price, volume}], bidList:[{price, volume}]
    topic 105:  Seems to be 102 and 103
    topic 106:  Seems to be 102 (and 103 sometimes depending on symbol/exchange)
    topic 107:  Seems to be 103 and 104
    topic 108:  Seems to be 103 and 104 and T: depth:{ntvAggAskList:[{price:, volume}], ntvAggBidList:[{price:,volume:}]}}
    """

    def _make_client(self, client_id):
        """Create an MQTT client compatible with paho-mqtt v1.x and v2.x."""
        try:
            return mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION1,
                client_id=client_id,
                transport='websockets',
            )
        except (AttributeError, TypeError):
            # paho-mqtt < 2.0 has no CallbackAPIVersion
            return mqtt.Client(client_id=client_id, transport='websockets')

    def _setup_callbacks(self):
        """
        Has to be done this way to have them live in a class and not require self as the first parameter
        Python is kind enough to hold onto a copy of self for the callbacks to use later on
        return: addresses of the call backs
        """
        def on_connect(client, userdata, flags, rc, *args):
            """
            The callback for when the client receives a CONNACK response from the server.
            """
            with self.oncon_lock:
                if self.debug_flg:
                    print("Connected with result code "+str(rc))
                if rc != 0:
                    raise ValueError("Connection Failed with rc:"+str(rc))

        def on_order_message(client, userdata, msg, *args):
            #{tickerId, orderId, filledQuantity, orderType, orderStatus}
            with self.onmsg_lock:
                topic = json.loads(msg.topic)
                data = json.loads(msg.payload)
                if self.debug_flg:
                    print(f'topic: {topic} ----- payload: {data}')

                if self.order_func is not None:
                    self.order_func(topic, data)

        def on_price_message(client, userdata, msg, *args):
            with self.onmsg_lock:
                try:
                    topic = json.loads(msg.topic)
                    data = json.loads(msg.payload)
                    if self.debug_flg:
                        print(f'topic: {topic} ----- payload: {data}')

                    if self.price_func is not None:
                        self.price_func(topic, data)

                except Exception as e:
                    logger.error("StreamConn price callback error: %s", e, exc_info=True)

        def on_subscribe(client, userdata, mid, granted_qos, *args):
            """
            The callback for when the client receives a SUBACK response from the server.
            """
            with self.onsub_lock:
                if self.debug_flg:
                    print(f"subscribe accepted with QOS: {granted_qos} with mid: {mid}")

        def on_unsubscribe(client, userdata, mid, *args):
            """
              The callback for when the client receives a UNSUBACK response from the server.
              """
            with self.onsub_lock:
                if self.debug_flg:
                    print(f"unsubscribe accepted with mid: {mid}")

        def on_disconnect(client, userdata, rc, *args):
            with self.oncon_lock:
                if self.debug_flg:
                    print(f"Disconnected with rc={rc}")
                logger.info("StreamConn disconnected (rc=%s)", rc)
                if self.disconnect_func is not None:
                    self.disconnect_func(rc)
        #-------- end message callbacks
        return on_connect, on_subscribe, on_price_message, on_order_message, on_unsubscribe, on_disconnect


    def connect(self, did, access_token=None) :
            if access_token is None:
                say_hello = {"header":
                                 {"did": did,
                                  "hl": "en",
                                  "app": "desktop",
                                  "os": "web",
                                  "osType": "windows"}
                             }
            else:
                say_hello = {"header":
                                 {"access_token": access_token,
                                  "did": did,
                                  "hl": "en",
                                  "app": "desktop",
                                  "os": "web",
                                  "osType": "windows"}
                             }


            #Has to be done this way to have them live in a class and not require self as the first parameter
            #in the callback functions
            on_connect, on_subscribe, on_price_message, on_order_message, on_unsubscribe, on_disconnect = self._setup_callbacks()

            if not access_token is None:
                # no need to listen to order updates if you don't have a access token
                # paper trade order updates are not send down this socket, I believe they
                # are polled every 30=60 seconds from the app

                self.client_order_upd = self._make_client(did)
                self.client_order_upd.on_connect = on_connect
                self.client_order_upd.on_subscribe = on_subscribe
                self.client_order_upd.on_message = on_order_message
                self.client_order_upd.on_disconnect = on_disconnect
                self.client_order_upd.tls_set_context()
                # this is a default password that they use in the app
                self.client_order_upd.username_pw_set('test', password='test')
                self.client_order_upd.connect('wspush.webullbroker.com', 443, 30)
                #time.sleep(5)
                self.client_order_upd.loop_start()  # runs in a second thread
                print('say hello')
                self.client_order_upd.subscribe(json.dumps(say_hello))
                #time.sleep(5)

            self.client_streaming_quotes = self._make_client(did)
            self.client_streaming_quotes.on_connect = on_connect
            self.client_streaming_quotes.on_subscribe = on_subscribe
            self.client_streaming_quotes.on_unsubscribe = on_unsubscribe
            self.client_streaming_quotes.on_message = on_price_message
            self.client_streaming_quotes.on_disconnect = on_disconnect
            self.client_streaming_quotes.tls_set_context()
            #this is a default password that they use in the app
            self.client_streaming_quotes.username_pw_set('test', password='test')
            self.client_streaming_quotes.connect('wspush.webullbroker.com', 443, 30)
            #time.sleep(5)
            self.client_streaming_quotes.loop()
            #print('say hello')
            self.client_streaming_quotes.subscribe(json.dumps(say_hello))
            #time.sleep(5)
            self.client_streaming_quotes.loop()
            #print('sub ticker')


    def run_blocking_loop(self):
            """
            this will never return, you need to put all your processing in the on message function
            """
            self.client_streaming_quotes.loop_forever()

    def run_loop_once(self):
        try:
            self.client_streaming_quotes.loop()
        except Exception as e:
            logger.error("StreamConn loop error: %s", e, exc_info=True)

    def subscribe(self, tId=None, level=105):
        #you can only use curly brackets for variables in a f string
        self.client_streaming_quotes.subscribe('{'+f'"tickerIds":[{tId}],"type":"{level}"'+'}')
        self.client_streaming_quotes.loop()


    def unsubscribe(self, tId=None, level=105):
        self.client_streaming_quotes.unsubscribe(f'["type={level}&tid={tId}"]')
        #self.client_streaming_quotes.loop() #no need for this, you should already be in a loop

    def connect_background(self, did, access_token=None):
        """Connect using loop_start() — returns immediately, paho runs in background thread.

        Use this for the streaming integration where the main thread needs to
        remain free for the event loop.  After calling this, use subscribe()
        then run_blocking_loop() is NOT needed — paho's background thread
        handles all IO.
        """
        if access_token is None:
            say_hello = {"header":
                             {"did": did,
                              "hl": "en",
                              "app": "desktop",
                              "os": "web",
                              "osType": "windows"}
                         }
        else:
            say_hello = {"header":
                             {"access_token": access_token,
                              "did": did,
                              "hl": "en",
                              "app": "desktop",
                              "os": "web",
                              "osType": "windows"}
                         }

        on_connect, on_subscribe, on_price_message, on_order_message, on_unsubscribe, on_disconnect = self._setup_callbacks()

        if access_token is not None:
            self.client_order_upd = self._make_client(did)
            self.client_order_upd.on_connect = on_connect
            self.client_order_upd.on_subscribe = on_subscribe
            self.client_order_upd.on_message = on_order_message
            self.client_order_upd.on_disconnect = on_disconnect
            self.client_order_upd.tls_set_context()
            self.client_order_upd.username_pw_set('test', password='test')
            self.client_order_upd.connect('wspush.webullbroker.com', 443, 30)
            self.client_order_upd.loop_start()
            self.client_order_upd.subscribe(json.dumps(say_hello))

        self.client_streaming_quotes = self._make_client(did)
        self.client_streaming_quotes.on_connect = on_connect
        self.client_streaming_quotes.on_subscribe = on_subscribe
        self.client_streaming_quotes.on_unsubscribe = on_unsubscribe
        self.client_streaming_quotes.on_message = on_price_message
        self.client_streaming_quotes.on_disconnect = on_disconnect
        self.client_streaming_quotes.tls_set_context()
        self.client_streaming_quotes.username_pw_set('test', password='test')
        self.client_streaming_quotes.connect('wspush.webullbroker.com', 443, 30)
        self.client_streaming_quotes.loop_start()
        time.sleep(0.5)  # brief pause for CONNACK
        self.client_streaming_quotes.subscribe(json.dumps(say_hello))

    def disconnect(self):
        """Cleanly stop both MQTT clients."""
        for client in (self.client_streaming_quotes, self.client_order_upd):
            if client is not None:
                try:
                    client.loop_stop()
                    client.disconnect()
                except Exception as e:
                    logger.debug("StreamConn disconnect cleanup: %s", e)
        self.client_streaming_quotes = None
        self.client_order_upd = None


if __name__ == '__main__':
    webull = webull(cmd=True)

    # for demo purpose
    webull.login('xxxxxx@xxxx.com', 'xxxxx')
    webull.get_trade_token('xxxxxx')
    # set self.account_id first
    webull.get_account_id()
    # webull.place_order('NKTR', 21.0, 1)
    orders = webull.get_current_orders()
    for order in orders:
        # print(order)
        webull.cancel_order(order['orderId'])
    # print(webull.get_serial_id())
    # print(webull.get_ticker('BABA'))

    #test streaming
    nyc = timezone('America/New_York')
    def on_price_message(topic, data):
        print (data)
        print(f"Ticker: {topic['tickerId']}, Price: {data['deal']['price']}, Volume: {data['deal']['volume']}", end='', sep='')
        if 'tradeTime' in data:
            print(', tradeTime: ', data['tradeTime'])
        else:
            tradetime = data['deal']['tradeTime']
            current_dt = datetime.today().astimezone(nyc)
            ts = current_dt.replace(hour=int(tradetime[:2]), minute=int(tradetime[3:5]), second=0, microsecond=0)
            print(', tradeTime: ', ts)

    def on_order_message(topic, data):
        print(data)


    conn = StreamConn(debug_flg=True)
    # set these to a processing callback where your algo logic is
    conn.price_func = on_price_message
    conn.order_func = on_order_message

    if not webull._access_token is None and len(webull._access_token) > 1:
        conn.connect(webull._did, access_token=webull._access_token)
    else:
        conn.connect(webull._did)

    conn.subscribe(tId='913256135') #AAPL
    conn.run_loop_once()
    conn.run_blocking_loop() #never returns till script crashes or exits
