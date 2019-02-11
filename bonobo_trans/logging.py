import logging 
import mondrian 
import bonobo_trans

mondrian.setup(excepthook=True)
logger = logging.getLogger(bonobo_trans.__name__)