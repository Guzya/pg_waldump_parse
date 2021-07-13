''' 
Анализируем результат работы утилиты pg_waldump.

При выполнении pg_waldump НЕ использовать ключ -b.

Вывести результаты работы pg_waldump в файл и передать его утилите (python3 wal.py --f waldump.log)

Из строки:  rmgr: Btree       len (rec/tot):     80/    80, tx:      94353, lsn: 4/46000058, prev 4/45FFFEE0, desc: INSERT_LEAF off 100, blkref #0: rel 1663/607671/608022 blk 11

делаем словарь:

id            - номер строки в файле
rmgr          - кто инициировал запись в wal
len (rec/tot) - ???
tx            - номер транзакции
lsn           - lsn-текущий lsn-предыдущей записи
desc          - описание выполненой операции
rel           - отдельно от desc для дальнейшего вывода объектов PostgreSQL
'''

import datetime
import os
import logging
import argparse


def main(wal_file):
    
    # читаем переданный файл и строим по нему словарь
    wal_dic = []
    i = 0
    with open(wal_file,'r') as f:
        for line in f.readlines():
            i = i + 1
            d_line = {}
            d_line['id'] = i
            d_line['rmgr'] = line[line.find('rmgr:') + 6:line.find('len')].strip()
            d_line['len_(rec/tot)'] = line[line.find('len (rec/tot):') + 15:line.find('tx')].strip()
            d_line['tx'] = line[line.find('tx:') + 4:line.find('lsn')].strip()
            d_line['lsn'] = line[line.find('lsn:') + 5:line.find('desc')].strip()
            d_line['desc'] = line[line.find('desc:') + 6:].strip()  
            d_line['rel'] = line[line.rfind('rel') + 3:line.rfind('blk')].strip()
            wal_dic.append(d_line)
            
    logger.info('Считано строк: {}'.format(i))        
    
    # ищем записи Transaction, отмечающие либо COMMIT, либо ROLLBACK(ABORT) транзакции 
    txs = [] 
    commits = []
    rollbacks = []
    othe_cmd = []
           
    for line in wal_dic:
        if line['rmgr'] == 'Transaction':
            logger.info('Найдена запись Transaction: {}'.format(line['tx'].strip(',')))
            txs.append({'id':line['id'], 'tx':line['tx'].strip(',')})
            if 'COMMIT' in line['desc']:
                commits.append({'id':line['id'], 'tx':line['tx'].strip(',')})
            elif 'ABORT' in line['desc']:
                rollbacks.append({'id':line['id'], 'tx':line['tx'].strip(',')})
            else :
                othe_cmd.append({'id':line['id'], 'tx':line['tx'].strip(',')})

    logger.info('Записей Transaction: {}'.format(len(txs))) 
    logger.info('-------------COMMIT: {}'.format(len(commits))) 
    logger.info('-----------ROLLBACK: {}'.format(len(rollbacks))) 
    logger.info('--------------OTHER: {}'.format(len(othe_cmd)))        
    
    # ищем по номеру транзакции записи относящиеся к этой транзакции
    wal_commits = []
    wal_rollbacks = []
    wal_othe = []
    
    for line in wal_dic:
        for tx in commits:
            if int(line['id']) < int(tx['id']) and line['tx'].strip(',') == tx['tx']:
                wal_commits.append(line)
                break
                
        for tx in rollbacks:
            if int(line['id']) < int(tx['id']) and line['tx'].strip(',') == tx['tx']:
                wal_rollbacks.append(line)
                break
                
        for tx in othe_cmd:
            if int(line['id']) < int(tx['id']) and line['tx'].strip(',') == tx['tx']:
                wal_othe.append(line)
                break                            
    
    # выводим собранную информацию
    logger.info('Записей найдено.') 
    logger.info('-------------COMMIT: {}'.format(len(wal_commits))) 
    logger.info('-----------ROLLBACK: {}'.format(len(wal_rollbacks))) 
    logger.info('--------------OTHER: {}'.format(len(wal_othe)))    
    logger.info('************************************************************')
    
    logger.info('Записи соответствующие записям COMMIT:') 
    for line in wal_commits:
         logger.info(line)       
    
    logger.info('************************************************************')     
    logger.info('Записи соответствующие записям ROLLBACK:')
    for line in wal_rollbacks:
         logger.info(line) 

    logger.info('************************************************************')
    logger.info('Записи соответствующие записям OTHER:')
    for line in wal_othe:
         logger.info(line) 
    
    logger.info('************************************************************')
    logger.info('************************************************************')
    
    logger.info('Найденные объекты PostgreSQL по которым найден COMMIT (tbs/db/oid : кол.):')
    
    bd_objects = {}
    
    for line in wal_commits:        
        if line['rel'] in bd_objects.keys():
            bd_objects[line['rel']] = bd_objects[line['rel']] + 1
        else:
            bd_objects[line['rel']] = 1
        
    for key,value in bd_objects.items():
        logger.info('{} : {}'.format(key,value))





if __name__ == '__main__':
	
	
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
	
    formatLogger = logging.Formatter('%(asctime)s: %(name)-12s: %(funcName)-17s: %(levelname)-8s: %(message)s')
    	
    filehandler = logging.FileHandler('wal-{}.log'.format(datetime.datetime.now().strftime("%Y.%m.%d_%H:%M")))
    filehandler.setLevel(logging.INFO)
    filehandler.setFormatter(formatLogger)
	
    logger.addHandler(filehandler)
    
    # Парсер аргументов коммандной строки
    parser = argparse.ArgumentParser(description='Справка по аргументам!')
    parser.add_argument("--f", type=str, help="File for parsing")
    
        
    parser.add_argument("--console", choices=["yes", "no"],
        default="no", type=str, help="output log in console, default \"no\"")
    
    args = parser.parse_args()
    
    if args.console == 'yes':
        formatConsole = logging.Formatter('%(asctime)s: %(levelname)-6s: %(message)s')	
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(formatConsole)
        logger.addHandler(console)
    
	
    logger.info('Начало работы ------------------------------------- ')	
    startTime = datetime.datetime.now()
    
    if not os.path.exists(args.f):
        logger.error('Файл "{}" не существует.'.format(args.f))
        exit(1)
      
   
    main(args.f)
        
    stopTime = datetime.datetime.now()    
    logger.info('Окончание работы ------------------------------------- ' )
    logger.info('Времы выполнения скрипта: ' + str(stopTime - startTime))
