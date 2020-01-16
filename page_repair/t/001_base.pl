use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 9;

my $master = get_new_node('master');
$master->init(
    allows_streaming => 1);
my $pgdata = $master->data_dir;

# Control file should know that checksums are disabled.
command_like(
    [ 'pg_controldata', $pgdata ],
    qr/Data page checksum version:.*0/,
    'checksums disabled in control file');

# Enable checksums.
command_ok([ 'pg_checksums', '--enable', '--no-sync', '-D', $pgdata ],
	   "checksums successfully enabled in cluster");

# Successive attempt to enable checksums fails.
command_fails([ 'pg_checksums', '--enable', '--no-sync', '-D', $pgdata ],
	      "enabling checksums fails if already enabled");


# Populate table
$master->start;
$master->safe_psql(
    'postgres',
    'CREATE EXTENSION page_repair;
    CREATE TABLE test WITH (autovacuum_enabled = off) AS SELECT generate_series(1,1000) as i;');

$master->backup('backup');

# Initialize standby node
my $standby = get_new_node('standby');
$standby->init_from_backup($master, 'backup',
			   has_streaming => 1);
$standby->start;

my $file_corrupted =
    $master->safe_psql('postgres', "SELECT pg_relation_filepath('test');");
my $relfilenode_corrupted = $master->safe_psql(
    'postgres',
    "SELECT relfilenode FROM pg_class WHERE relname = 'test';");

# Set page header and block size
my $pageheader_size = 24;
my $block_size = $master->safe_psql('postgres', 'SHOW block_size;');
$master->stop;
$standby->stop;

# Time to create some corruption
open my $file, '+<', "$pgdata/$file_corrupted";
seek($file, $pageheader_size, 0);
syswrite($file, "\0\0\0\0\0\0\0\0\0");
close $file;

# Check checksum verification fails due to corrupted relation
$master->command_checks_all(
    [
     'pg_checksums', '--check',
     '-D',           $pgdata,
     '--filenode',   $relfilenode_corrupted
    ],
    1,
    [qr/Bad checksums:.*1/],
    [qr/checksum verification failed/],
    "fails with corrupted data for single relfilenode"
    );

$master->start;
$standby->start;

# Repair the corrupted page
my $standby_connstr = $standby->connstr;
$master->safe_psql(
	'postgres',
	"SELECT pg_repair_page('test', 0, '$standby_connstr')");

$master->stop;
$standby->stop;

# Checksums pass on a fixed relation
command_ok([
	'pg_checksums', '--check',
	'-D', $pgdata,
	'--filenode', $relfilenode_corrupted
		   ],
		   "succeeds with offline cluster");
