package Logger;
use strict;
use File::Basename;
use File::Path;
use File::Spec;
use Config::IniFiles;

our(@ISA, @EXPORT);
@ISA = qw(Exporter);
@EXPORT = qw(debug info warning error fatal);
#日志级别 OFF|ALL|DEBUG|INFO|WARNING|ERROR|FATAL
my %levels = (
	OFF   => 0,
	ALL   => 1,
	DEBUG => 2,
	INFO  => 3,
	WARNING  => 4,
	ERROR => 5,
	FATAL => 6
);
#日志的全局配置文件(初始化方法中的配置会覆盖全局配置)
my $cfgFile = File::Spec->catfile($ENV{"AUTO_HOME"},'config','ecif_logger.ini');
#模块的全局配置
my %cfg;
if(-e $cfgFile){
	tie %cfg,'Config::IniFiles',(-file => $cfgFile, -nomultiline => 1);
}
#日志配置属性，如果配置文件中未提供相应配置则取其默认值
my $level = $cfg{logger}{level} || 'INFO';				#日志级别
my $appenderStr = $cfg{logger}{appender} || 'screen';	#日志附加器,默认输出到屏幕
my $screen = undef;										#输出到屏幕标记，为0时不输出到屏幕
my $file = undef;										#输出到文件标记，为0时不输出到文件
my $logFile = $cfg{logger}{file} || undef;				#输出的文件
my $layout = $cfg{logger}{layout} || '%m%n';            #日志输出的格式，默认只输出原始日志
setAppender($appenderStr);

#日志初始化方法
sub init{
	shift;
	my %parms = @_;
#	初始化方法没有的参数则从全局参数中取值,如果全局参数也没有配置则取其默认值
	$level = $parms{level} || $level;
	$level = uc($level);
	$appenderStr = $parms{appender} || $appenderStr;
	$logFile = $parms{file} || $logFile;
	$layout = $parms{layout} || $layout;
#	设置日志级别
	my $levelNum = getLevelNum($level);
	if($levelNum == -1){die "the level $level is not supported in package ".__PACKAGE__;}
#	设置日志附加器，默认输出到屏幕
	setAppender($appenderStr);
}

#Logger构造方法(不提供直接的构造方法)
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
#返回日志级别对应的数字表示，-1表示不存在的日志级别
sub getLevelNum{
	my $level = uc(shift);
	if(exists $levels{$level}){return $levels{$level};}
	return -1;
}

#创建日志文件，成功返回1失败退出程序
sub createLogFile{
	my $logFile = shift;
	if(-e $logFile){return 1;}
	my $dir = dirname $logFile;
	unless(-e $dir){
		eval{
			mkpath($dir); #递归创建目录
		};
		if($@){
			#递归创建目录失败时退出程序
			die "mkdir $dir failed in package ".__PACKAGE__;
		}
	}
	open LOGFILE,">>$logFile" or die "Can't create $logFile in package ".__PACKAGE__;
	close LOGFILE;
	return 1;
}


=log format
#格式化日志信息
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
	$format_msg =~ s/\%m/$message/g;   #最后替换日志信息
	
	return $format_msg;
}

#输出日志信息
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

	#日期格式如 [2011-09-01 13:10:48]
	my $fullTime = sprintf(
		"%04d-%02d-%02d %02d:%02d:%02d",
		$year += 1900,
		${mon}+1, ${mday}, ${hour}, ${min}, ${sec}
	);
	return $fullTime;
}

1;

__END__