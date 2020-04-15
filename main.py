import logger
import minidb
import exception

lh = logger.log_init()
db = minidb.MiniDB()

while True:
    try:
        new_command = input("enter command: ")
    except KeyboardInterrupt:
        exit()
    try:
        result = db.command_parser(new_command)
        if db.get_close():
            exit()
        if result:
            print(result)
    except exception.MiniDBException as err:
        lh.error(err)
