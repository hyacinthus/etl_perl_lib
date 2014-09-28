package Logger;
use strict;
use File::Basename;
use File::Path;
use File::Spec;
use Config::IniFiles;

our(@ISA, @EXPORT);
@ISA = qw(Exporter);
@EXPORT = qw(debug info warning error fatal);
#��־���� OFF|ALL|DEBUG|INFO|WARNING|ERROR|FATAL
my %levels = (
	OFF   => 0,
	ALL   => 1,
	DEBUG => 2,
	INFO  => 3,
	WARNING  => 4,
	ERROR => 5,
	FATAL => 6
);
#��־��ȫ�������ļ�(��ʼ�������е����ûḲ��ȫ������)
my $cfgFile = File::Spec->catfile($ENV{"AUTO_HOME"},'config','ecif_logger.ini');
#ģ���ȫ������
my %cfg;
if(-e $cfgFile){
	tie %cfg,'Config::IniFiles',(-file => $cfgFile, -nomultiline => 1);
}
#��־�������ԣ���������ļ���δ�ṩ��Ӧ������ȡ��Ĭ��ֵ
my $level = $cfg{logger}{level} || 'INFO';				#��־����
my $appenderStr = $cfg{logger}{appender} || 'screen';	#��־������,Ĭ���������Ļ
my $screen = undef;										#�������Ļ��ǣ�Ϊ0ʱ���������Ļ
my $file = undef;										#������ļ���ǣ�Ϊ0ʱ��������ļ�
my $logFile = $cfg{logger}{file} || undef;				#������ļ�
my $layout = $cfg{logger}{layout} || '%m%n';            #��־����ĸ�ʽ��Ĭ��ֻ���ԭʼ��־
setAppender($appenderStr);

#��־��ʼ������
sub init{
	shift;
	my %parms = @_;
#	��ʼ������û�еĲ������ȫ�ֲ�����ȡֵ,���ȫ�ֲ���Ҳû��������ȡ��Ĭ��ֵ
	$level = $parms{level} || $level;
	$level = uc($level);
	$appenderStr = $parms{appender} || $appenderStr;
	$logFile = $parms{file} || $logFile;
	$layout = $parms{layout} || $layout;
#	������־����
	my $levelNum = getLevelNum($level);
	if($levelNum == -1){die "the level $level is not supported in package ".__PACKAGE__;}
#	������־��������Ĭ���������Ļ
	setAppender($appenderStr);
}

#Logger���췽��(���ṩֱ�ӵĹ��췽��)
sub new {
	die "THIS CLASS ISN'T FOR DIRECT USE. PLEASE CHECK " .__PACKAGE__ ;
}

sub debug{
	my $message = $_[0];
	my $format  = $_[1] || undef;
	my $levelNum = getLevelNum($level);
	if($levelNum <= 0){return}
	if(getLevelNum('DEBUG') >= $levelNum){
		printMessage($message,'DEBUG',$format);
	}
}

sub info{
	my $message = $_[0];
	my $format  = $_[1] || undef;
	my $levelNum = getLevelNum($level);
	if($levelNum <= 0){return}
	if(getLevelNum('INFO') >= $levelNum){
		printMessage($message,'INFO ',$format);
	}
}

sub warning{
	my $message = $_[0];
	my $format  = $_[1] || undef;
	my $levelNum = getLevelNum($level);
	if($levelNum <= 0){return}
	if(getLevelNum('WARNING') >= $levelNum){
		printMessage($message,'WARN ',$format);
	}
}

sub error{
	my $message = $_[0];
	my $format  = $_[1] || undef;
	my $levelNum = getLevelNum($level);
	if($levelNum <= 0){return}
	if(getLevelNum('ERROR') >= $levelNum){
		printMessage($message,'ERROR',$format);
	}
}

sub fatal{
	my $message = $_[0];
	my $format  = $_[1] || undef;
	my $levelNum = getLevelNum($level);
	if($levelNum <= 0){return}
	if(getLevelNum('FATAL') >= $levelNum){
		printMessage($message,'FATAL',$format);
	}
	die formatMessage($message,'FATAL','[%d %l]%m%n');
}

sub getLogFile{
	return $logFile;
}

sub getLevel{
	return $level;
}

sub setAppender{
	$appenderStr = $_[0];
	my @appenders = split(',',$appenderStr);
	$screen = 0;
	$file = 0;
	for my $appender (@appenders){
		if($appender eq 'screen'){
			$screen = 1;
		}elsif($appender eq 'file'){
			$file = 1;
		}else{
			die "the appender $appender is not supported in package ".__PACKAGE__;
		}
	}
}
sub getAppender{
	return $appenderStr;
}
#������־�����Ӧ�����ֱ�ʾ��-1��ʾ�����ڵ���־����
sub getLevelNum{
	my $level = uc(shift);
	if(exists $levels{$level}){return $levels{$level};}
	return -1;
}

#������־�ļ����ɹ�����1ʧ���˳�����
sub createLogFile{
	my $logFile = shift;
	if(-e $logFile){return 1;}
	my $dir = dirname $logFile;
	unless(-e $dir){
		eval{
			mkpath($dir); #�ݹ鴴��Ŀ¼
		};
		if($@){
			#�ݹ鴴��Ŀ¼ʧ��ʱ�˳�����
			die "mkdir $dir failed in package ".__PACKAGE__;
		}
	}
	open LOGFILE,">>$logFile" or die "Can't create $logFile in package ".__PACKAGE__;
	close LOGFILE;
	return 1;
}


=log format
#��ʽ����־��Ϣ
%d  date & time in yyyy-mm-dd hh:mm:ss fomat 
%l  The logger level
%m  The message to be logged
%M  Method or function where the logging request was issued
%L  Line number within the file where the log statement was issued
$f  File where the logging event occurred
%F  File where the logging event occurred(with absolute path)
%C  Fully qualified package (or class) name of the caller
%p  A literal percent (%) sign
%n  Newline (OS-independent)
%t  A literal tab sign
=cut
sub formatMessage{
	my $message = $_[0];
	my $curLev  = $_[1];
	my $format  = $_[2];
	if(($format && $format eq '%m') || !defined $layout || $layout eq '%m'){
	    return $message;
	}
	my($package, $file, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask) = caller(2);
	$subroutine = (caller(3))[3] || 'main';
	my $format_msg = $format || $layout;
	my $full_time  = getFullTime();
	my $filename   = '';
	if(defined $file){
    	$filename = basename($file);
	}
	
	$format_msg =~ s/\%d/$full_time/g;
	$format_msg =~ s/\%l/$curLev/g;
	$format_msg =~ s/\%M/$subroutine/g;
	$format_msg =~ s/\%L/$line/g;
	$format_msg =~ s/\%f/$filename/g;
	$format_msg =~ s/\%F/$file/g;
	$format_msg =~ s/\%C/$package/g;
	$format_msg =~ s/\%n/\n/g;
	$format_msg =~ s/\%t/\t/g;
	$format_msg =~ s/\%p/\%/g;
	$format_msg =~ s/\%m/$message/g;   #����滻��־��Ϣ
	
	return $format_msg;
}

#�����־��Ϣ
sub printMessage{
	my $message = $_[0];
	my $level   = $_[1];
	my $format  = $_[2];
	my $screen  = $screen;
	my $msg = formatMessage($message,$level,$format);
	if($screen){print $msg};
	if($file){
		unless($logFile){
			#warn("have not specify logfile,we will not print log to file");
			return;
		};
		unless(-e $logFile){createLogFile($logFile);}
		open LOG,">>$logFile";
		print LOG $msg or die "write log to $logFile failed ($!)";
		close LOG;
	}
}

sub getFullTime {
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) =
	  localtime( time() );

	#���ڸ�ʽ�� [2011-09-01 13:10:48]
	my $fullTime = sprintf(
		"%04d-%02d-%02d %02d:%02d:%02d",
		$year += 1900,
		${mon}+1, ${mday}, ${hour}, ${min}, ${sec}
	);
	return $fullTime;
}

1;

__END__