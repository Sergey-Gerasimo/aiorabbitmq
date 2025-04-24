import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def set_up_logger(new_logger): 
    global logger 

    logger = new_logger

