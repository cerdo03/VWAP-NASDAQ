import struct
import os
from tqdm import tqdm

HOUR = 3.6e12 # an hour in nanoseconds
PREV_TIME = 0
CURR_TIME = 0
COUNT = 0

buy_orders = dict()  # order_id: [price,qty,locate_id]
stock_names = dict()  # locate_id: symbol
executed_orders = dict()  # locate_id: [price, qty, order_id,match_id]


def read_itch_file(data_path):
    global PREV_TIME, CURR_TIME, HOUR
        
    with open(data_path, 'rb') as file:
        # Read the entire content into memory
        content = file.read()
        file_size = len(content)

    # Use struct to unpack binary data, we are reading msg_size and then read only that many 
    # specific bytes for that particular message as the binary data file is pretty large
    offset = 0
    msg_size = struct.unpack('>H', content[offset:offset+2])[0]

    with tqdm(total=file_size, unit='B', unit_scale=True, dynamic_ncols=True) as pbar:
        while msg_size:
            offset += 2 # Move the offset past the previous message size
            message = content[offset:offset+msg_size]
            message_type = chr(message[0])
        
            parse_message(message, message_type)

            if CURR_TIME - PREV_TIME >= HOUR:
                PREV_TIME = CURR_TIME
                print('\nTime: ', int(CURR_TIME/HOUR),'hours from midnight')
                print_vwap(CURR_TIME)
        

            # Move the offset to the next message size
            offset += msg_size
            msg_size = struct.unpack('>H', content[offset:offset+2])[0]
            pbar.update(offset)
        
        
# Function to parse messages
def parse_message(msg, msg_type):
    global CURR_TIME
    global PREV_TIME
    global HOUR
    global COUNT
    
    try:
        # For message type S, event code at 11th position. 
        # Event code C represents: End of Messages. This is always the last message sent in any trading day.
        if msg_type == "S":
            if chr(msg[11])=='C':
                CURR_TIME = int.from_bytes(msg[5:11], 'big')
                print('\nEnd of the day: ', float(CURR_TIME/HOUR), 'hours from midnight')
                print_vwap(CURR_TIME)
                exit()
            
            if chr(msg[11])=='S':
                print("\nStart of System Hours: NASDAQ is open")
            
            if chr(msg[11])=='Q':
                print("\nStart of Market Hours")
            
            if chr(msg[11])=='M':
                print("\nEnd of Market Hours")
            
            if chr(msg[11])=='E':
                print("\nEnd of System Hours: No new order would be taken")
            
        # At the start of each trading day, Nasdaq disseminates stock directory messages for all active 
        # symbols in the Nasdaq execution system.
        elif msg_type == "R": 
            locate_id = int.from_bytes(msg[1:3], 'big')
            stock_name = msg[11:19].decode('ascii', 'ignore').strip()
            stock_names[locate_id] = stock_name
            executed_orders[locate_id] = []
        
        #Buy order: Adding order to buy_orders dictionary
        elif (msg_type == "A" or msg_type == "F") and chr(msg[19]) == 'B':
            msg = msg[:36] #Trimming down till 36 size as in case msg_type is F, message after 36 is Attribution
            CURR_TIME = int.from_bytes(msg[5:11], 'big')
            _, locate_id, _, _, order_id, _, qty, _, price = struct.unpack('>sHH6sQsI8sI', msg)
            buy_orders[order_id] = [price, qty, locate_id]
        
        #Executed order(Partial/Complete) at : Adding order to executed_orders dictionary,
        # if partial, subtracting executed quantity from buy order else delete corresponding buy order
        elif msg_type == "E":
            CURR_TIME = int.from_bytes(msg[5:11], 'big')
            
            unpacked_data = struct.unpack('>sHH6sQIQ', msg)
            locate_id = unpacked_data[1]
            order_id = unpacked_data[4]
            executed_qty = unpacked_data[5]
            match_id = unpacked_data[6]
            
            price = buy_orders.get(order_id)[0]
            qty = buy_orders.get(order_id)[1]
            
            if qty > executed_qty:
                buy_orders.update({order_id:[price, qty-executed_qty, locate_id]})
            else:
                del buy_orders[order_id]
            
            executed_orders[locate_id] = [[price, qty, 0, match_id]]

        # Order executed at price different from the initial display price. Nasdaq recommends that firms ignore messages marked as non-
        # printable (N) to prevent double counting.
        elif msg_type == "C" and chr(msg[31]) != 'N':
            CURR_TIME = int.from_bytes(msg[5:11], 'big')
            
            unpacked_data = struct.unpack('>sHH6sQIQsI', msg)
            locate_id = unpacked_data[1]
            order_id = unpacked_data[4]
            executed_qty = unpacked_data[5]
            match_id = unpacked_data[6]
            executed_price = unpacked_data[8]
            
            price = buy_orders.get(order_id)[0]
            qty = buy_orders.get(order_id)[1]
            
            if qty > executed_qty:
                buy_orders.update({order_id:[price, qty-executed_qty, locate_id]})
            else:
                del buy_orders[order_id]
            
            executed_orders[locate_id] = [[executed_price, qty, 0, match_id]]

        # Order Cancel Message: Partial cancellation
        elif msg_type == "X":
            order_id, qty = struct.unpack(">QI", msg[11:23])

            if buy_orders.get(order_id) is not None:
                if buy_orders.get(order_id)[1] > 0:
                    buy_orders.get(order_id)[1] -= qty
                else:
                    del buy_orders[order_id]
                    
        # Order Delete Message
        elif msg_type == "D":
            unpacked_data = struct.unpack('>Q',msg[11:19])
            order_id = unpacked_data[0]
            if buy_orders.get(order_id) is not None:
                del buy_orders[order_id]
        
        # Order replace message: All remaining shares from the
        # original order are no longer accessible, and must be removed. The new order details are provided for the
        # replacement, along with a new order reference number which will be used henceforth
        elif msg_type == "U":
            unpacked_data = struct.unpack('>sHH6sQQII', msg)
            locate_id = unpacked_data[1]
            old_order_id = unpacked_data[4]
            new_order_id = unpacked_data[5]
            qty = unpacked_data[6]
            price = unpacked_data[7]
            
            buy_orders.pop(old_order_id, None)
            buy_orders[new_order_id] = [price, qty, locate_id]
        
        # Trade Message (Non---Cross)
        elif msg_type == "P" and chr(msg[19]) == 'B':
            CURR_TIME = int.from_bytes(msg[5:11], 'big')
            locate_id = int.from_bytes(msg[1:3], 'big')

            qty, _, price, match_id = struct.unpack('>I8sIQ', msg[20:])
            executed_orders[locate_id] = [[price, qty, 0, match_id]]
            
        # Cross Trade Message
        elif msg_type == "Q":
            locate_id = int.from_bytes(msg[1:3], 'big')
            CURR_TIME = int.from_bytes(msg[5:11], 'big')

            qty, _, price, match_id = struct.unpack('>Q8sIQ', msg[11:39])
            executed_orders[locate_id] = [[price, qty, 0, match_id]]
            
        # Broken trade message :  Firms that use the ITCH feed to create time-­­and-­­sales displays or
        # calculate market statistics should be prepared to process the broken trade message
        elif msg_type == "B":
            locate_id = int.from_bytes(msg[1:3], 'big')
            match_id = int.from_bytes(msg[11:19], 'big')
            orders = executed_orders.get(locate_id)
            if orders is not None:
                new_orders = list(filter(lambda a: a[3] != match_id, orders))
                executed_orders[locate_id] = new_orders
    except Exception as e:
        return

def print_vwap(cur_time):
    vwap = {}
    for locate_id, trades in executed_orders.items():
        volume, vp = 0, 0
        volume = sum(trade[1] for trade in trades)
        vp = sum(trade[0] * trade[1] for trade in trades)
        symbol = stock_names[locate_id]
        
        if volume > 0:
            vwap[symbol] = vp / (volume * 1e4)
            
    out_dir = 'out/'
    os.makedirs(out_dir, exist_ok=True)  # Ensuring the 'out/' directory exists

    fout = out_dir + str(int(cur_time / HOUR)) + ".txt"
    
    with open(fout, "w+") as fo:
        for k, v in vwap.items():
            fo.write(str(k) + ' ' + str(v) + '\n')
        


if __name__ == "__main__":
    # Input asking user for the path of the data file,
    print("Path example: /Users/suryashukla/Developer/data/01302019.NASDAQ_ITCH50.gz")
    data_path = input("Enter the full data path here: ")
    try:
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"The file at path {data_path} does not exist.")
        read_itch_file(data_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")