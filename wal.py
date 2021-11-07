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
    #commits = []
    #rollbacks = []
    #othe_cmd = []

    commits_count = 0
    rollbacks_count = 0
    othe_cmd_count = 0
    
    commit_txs = {}
    rollback_txs = {}
    othe_cmd_txs = {}
           
    for line in wal_dic:
        if line['rmgr'] == 'Transaction':
            logger.info('Найдена запись Transaction: {}'.format(line['tx'].strip(',')))
            txs.append({'id':line['id'], 'tx':line['tx'].strip(',')})
            if 'COMMIT' in line['desc']:
#                commits.append({'id':line['id'], 'tx':line['tx'].strip(',')})
                commits_count = commits_count + 1
                if line['tx'].strip(',') in commit_txs.keys():
                    commit_txs[line['tx'].strip(',')].append({'id':line['id'], 'tx':line['tx'].strip(',')})
                else:
                    commit_txs[line['tx'].strip(',')] = []
                    commit_txs[line['tx'].strip(',')].append({'id':line['id'], 'tx':line['tx'].strip(',')})
                
                    
            elif 'ABORT' in line['desc']:
#                rollbacks.append({'id':line['id'], 'tx':line['tx'].strip(',')})
                rollbacks_count = rollbacks_count + 1
                if line['tx'].strip(',') in rollback_txs.keys():
                    rollback_txs[line['tx'].strip(',')].append({'id':line['id'], 'tx':line['tx'].strip(',')})
                else:
                    rollback_txs[line['tx'].strip(',')] = []
                    rollback_txs[line['tx'].strip(',')].append({'id':line['id'], 'tx':line['tx'].strip(',')})
            else :
#                othe_cmd.append({'id':line['id'], 'tx':line['tx'].strip(',')})
                othe_cmd_count = othe_cmd_count + 1
                if line['tx'].strip(',') in othe_cmd_txs.keys():
                    othe_cmd_txs[line['tx'].strip(',')].append({'id':line['id'], 'tx':line['tx'].strip(',')})
                else:
                    othe_cmd_txs[line['tx'].strip(',')] = []
                    othe_cmd_txs[line['tx'].strip(',')].append({'id':line['id'], 'tx':line['tx'].strip(',')})

    logger.info('Записей Transaction: {}'.format(len(txs))) 
#    logger.info('-------------COMMIT: {}'.format(len(commits))) 
#    logger.info('-----------ROLLBACK: {}'.format(len(rollbacks))) 
#    logger.info('--------------OTHER: {}'.format(len(othe_cmd)))        
    logger.info('-------------COMMIT: {}'.format(commits_count)) 
    logger.info('-----------ROLLBACK: {}'.format(rollbacks_count)) 
    logger.info('--------------OTHER: {}'.format(othe_cmd_count))        
    
    
    logger_stats.info('Записей Transaction: {}'.format(len(txs))) 
    logger_stats.info('-------------COMMIT: {}'.format(commits_count)) 
    logger_stats.info('-----------ROLLBACK: {}'.format(rollbacks_count)) 
    logger_stats.info('--------------OTHER: {}'.format(othe_cmd_count))     
    
    # ищем по номеру транзакции записи относящиеся к этой транзакции
    logger.info('Ищем по номеру транзакции записи относящиеся к этой транзакции.')
    logger_stats.info('Ищем по номеру транзакции записи относящиеся к этой транзакции.')
    wal_commits = []
    wal_rollbacks = []
    wal_othe = []
    
    wal_dic_len = len(wal_dic)
    wal_dic_count = 0

    for line in wal_dic:
        
        wal_dic_count = wal_dic_count + 1
        if (wal_dic_count % 10000) == 0:
            logger.info('Обработанно записей: {} из {}'.format(wal_dic_count,wal_dic_len))
        
        #for tx in commits:
            #if int(line['id']) < int(tx['id']) and line['tx'].strip(',') == tx['tx']:
                #wal_commits.append(line)
                #break
        if line['tx'].strip(',') in commit_txs.keys():        
            for tx in commit_txs[line['tx'].strip(',')]:
                if int(line['id']) < int(tx['id']) and line['tx'].strip(',') == tx['tx']:
                    wal_commits.append(line)
                    break

        if line['tx'].strip(',') in rollback_txs.keys():        
            for tx in rollback_txs[line['tx'].strip(',')]:
                if int(line['id']) < int(tx['id']) and line['tx'].strip(',') == tx['tx']:
                    wal_rollbacks.append(line)
                    break


        if line['tx'].strip(',') in othe_cmd_txs.keys():        
            for tx in othe_cmd_txs[line['tx'].strip(',')]:
                if int(line['id']) < int(tx['id']) and line['tx'].strip(',') == tx['tx']:
                    wal_othe.append(line)
                    break
                                    
        #for tx in rollbacks:
            #if int(line['id']) < int(tx['id']) and line['tx'].strip(',') == tx['tx']:
                #wal_rollbacks.append(line)
                #break
                
        #for tx in othe_cmd:
            #if int(line['id']) < int(tx['id']) and line['tx'].strip(',') == tx['tx']:
                #wal_othe.append(line)
                #break                            
    
    # выводим собранную информацию
    logger.info('Записей найдено.') 
    logger.info('-------------COMMIT: {}'.format(len(wal_commits))) 
    logger.info('-----------ROLLBACK: {}'.format(len(wal_rollbacks))) 
    logger.info('--------------OTHER: {}'.format(len(wal_othe)))    
    logger.info('************************************************************')


    logger_stats.info('Записей найдено.') 
    logger_stats.info('-------------COMMIT: {}'.format(len(wal_commits))) 
    logger_stats.info('-----------ROLLBACK: {}'.format(len(wal_rollbacks))) 
    logger_stats.info('--------------OTHER: {}'.format(len(wal_othe)))    
    logger_stats.info('************************************************************')



    
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
    logger_stats.info('Найденные объекты PostgreSQL по которым найден COMMIT (tbs/db/oid : кол.):')
    
    bd_objects = {}
    
    for line in wal_commits:        
        if line['rel'] in bd_objects.keys():
            bd_objects[line['rel']] = bd_objects[line['rel']] + 1
        else:
            bd_objects[line['rel']] = 1
        
    for key,value in bd_objects.items():
        logger.info('{} : {}'.format(key,value))
        logger_stats.info('{} : {}'.format(key,value))





if __name__ == '__main__':
	
    date_file = datetime.datetime.now().strftime("%Y.%m.%d_%H:%M")
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
	
    formatLogger = logging.Formatter('%(asctime)s: %(name)-12s: %(funcName)-17s: %(levelname)-8s: %(message)s')
    	
    filehandler = logging.FileHandler('wal-{}.log'.format(date_file))
    filehandler.setLevel(logging.INFO)
    filehandler.setFormatter(formatLogger)
	
    logger.addHandler(filehandler)
    
    # Отдельный лог-файл для статистики
    logger_stats = logging.getLogger(__name__ + '-state')
    logger_stats.setLevel(logging.INFO)
    filehandler_stats = logging.FileHandler('wal_stats-{}.log'.format(date_file))
    filehandler_stats.setLevel(logging.INFO)
    filehandler_stats.setFormatter(formatLogger)
	
    logger_stats.addHandler(filehandler_stats)
    
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
    logger_stats.info('Начало работы ------------------------------------- ')
    startTime = datetime.datetime.now()
    
    if not os.path.exists(args.f):
        logger.error('Файл "{}" не существует.'.format(args.f))
        exit(1)
      
   
    main(args.f)
        
    stopTime = datetime.datetime.now()    
    logger.info('Окончание работы ------------------------------------- ' )
    logger.info('Времы выполнения скрипта: ' + str(stopTime - startTime))
    
    logger_stats.info('Окончание работы ------------------------------------- ' )
    logger_stats.info('Времы выполнения скрипта: ' + str(stopTime - startTime))
