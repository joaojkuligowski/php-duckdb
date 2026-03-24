--TEST--
Test DuckDB instance cache for file-backed databases
--FILE--
<?php
$path = __DIR__ . '/instance-cache.db';

@unlink($path);
@unlink($path . '.wal');

$db1 = new Fnvoid\DuckDB\DuckDB($path);
$db1->query('CREATE TABLE test_tbl (id INT)');
$db1->query('INSERT INTO test_tbl VALUES (42)');

$db2 = new Fnvoid\DuckDB\DuckDB($path);
$row = $db2->query('SELECT id FROM test_tbl')->fetchOne();

var_dump($row->id);
?>
--CLEAN--
<?php
$path = __DIR__ . '/instance-cache.db';
@unlink($path);
@unlink($path . '.wal');
?>
--EXPECT--
int(42)