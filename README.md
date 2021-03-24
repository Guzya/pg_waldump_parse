# pg_waldump_parse

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
