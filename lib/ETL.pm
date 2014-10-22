#!/usr/bin/perl
#########################################################################################################
#Function:              ETL 公用模块
#Author:                WuGang
#Date Time:
#History:
#Copyright              2011 VanceInfo All Rights Reserved.
#########################################################################################################
package ETL;
use strict;
use MIME::Base64;
use Crypt::RC4;
use DBI;
use Config::IniFiles;
use File::Spec;
use File::Basename;
use Time::Local;
use Logger;
use Exporter;
use SOAP::Lite;
use Encode::CN;
use File::Path;
use CharCrypt;
use IO::Handle;
use Fcntl ':flock';
use Time::HiRes;
use Digest::MD5;


##################################################
my %ecif_local_cfg;                 #文件中的配置信息
my %ecif_sys_cfg;                   #数据库中系统管理参数配置
my %db_cfg;                         #数据库连接配置信息
##################################################
BEGIN{
    my %module = (
        mswin32 => 'ECIF_NT',
        netware => 'ECIF_NT',
        linux   => 'ECIF_Unix'
    );

    my $module = $module{lc($^O)} || 'ECIF_Unix';
    require "$module.pm";
    our(@ISA, @EXPORT);
    @ISA = ("$module","Exporter");
    #导出子函数列表
    @EXPORT = qw(
        trim
        getCfg
        makePath
        getSysCfg
        dateCalc
        isValidDate
        getUserAndPwd
        getCurDate
        getCurTime
        getCurFormatTime
        getCurDateTime
        insertEventLog
        isWholeData
        sendEODMsg
        sendSMSMsg
        sendFTPMsg
        getMsgSendFlag
        getDbhByTag
        checkBrmsStat
        insertBrmsMessage
        checkBrmsMessage
        deleteBrmsMessage
        getDbNameByTag
    );
}
##################common function##################
#获取配置信息，如果文件中的配置和数据库中的配置冲突以谁为准待讨论（未实现）
#sub getECIFCfgVal{
#   my ($tag,$key) = @_;
#   my $val;
#   unless(%ecif_local_cfg){
#       tie %ecif_local_cfg,'Config::IniFiles',(-file => File::Spec->catfile($ENV{"AUTO_HOME"},'config','ecif_cfg.ini'));
#   }
#   $val = $ecif_local_cfg{$tag}{$key};
#   if($val){return $val;}
#   unless(%ecif_sys_cfg){
#       %ecif_sys_cfg = getSysAdminConfig();
#   }
#   return $ecif_sys_cfg{$tag}{$key};
#}

#获取本地文件中的配置
sub getCfg{
    my ($tag,$key) = @_;
#   采取懒加载的方式
    unless(%ecif_local_cfg){
        tie %ecif_local_cfg,'Config::IniFiles',(-file => File::Spec->catfile($ENV{"AUTO_HOME"},'config','ecif_cfg.ini'));
    }
#   如果没有给key，则把tag下所有的配置以hash的形式返回
    unless(defined $key){
        my $hash = $ecif_local_cfg{$tag};
        unless($hash){debug("the configFile do not have the tag '$tag'");return undef;}
        my %hash = %$hash;
        return %hash;
    }
    my $val = $ecif_local_cfg{$tag}{$key};
    unless(defined $val){debug("the configFile do not have the tag '$tag' and the key '$key'");}
    return trim($val);
}

#获取数据库系统管理参数配置
sub getSysCfg{
    my ($tag,$key) = @_;
#   采取懒加载的方式
    unless(%ecif_sys_cfg){
        %ecif_sys_cfg = getSysAdminConfig();
    }
    #   如果没有给key，则把tag下所有的配置以hash的形式返回
    unless(defined $key){
        my $hash = $ecif_sys_cfg{$tag};
        unless($hash){warning("the SysAdminCfg do not have the tag '$tag'");return undef;}
        my %hash = %$hash;
        return %hash;
    }
    my $val = $ecif_sys_cfg{$tag}{$key};
    unless(defined $val){warning("the SysAdminCfg do not have the tag '$tag' and the key '$key'");}
    return trim($val);
}

#日期计算
#参数 $date   计算的参照日期
#参数 $offset 与参照日期的偏移量，正数表示以后的日期，负数表示以前的日期
sub dateCalc{
    my ( $date, $offset ) = @_;
    unless(isValidDate($date)){return undef;} #首先校验参考日期的有效性
    my $loc_year  = substr( $date, 0, 4 );
    my $loc_month = substr( $date, 4, 2 );
    my $loc_mday  = substr( $date, 6, 2 );
    $loc_month -= 1;
    my $timenum =
      Time::Local::timelocal( 0, 0, 0, $loc_mday, $loc_month, $loc_year );
    my @timestr = localtime( $timenum + 86400 * $offset );
    my $year    = $timestr[5] + 1900;
    my $month   = $timestr[4] + 1;
    my $mday    = $timestr[3];

    #格式化为YYYYMMDD的形式
    my $yyyymmdd = sprintf( "%04d%02d%02d", ${year}, ${month}, ${mday} );
    return $yyyymmdd;
}

#校验 yyyymmdd 日期格式的有效性 有效返回 1 无效返回 0
sub isValidDate{
    my $date = $_[0];
    #长度必须为8且八位日期必须为数字
    unless((length($date) == 8) and ($date =~ m/\d{8}/)){return 0}
    my $yyyy = substr($date,0,4);
    my $mm = substr($date,4,2);
    my $dd = substr($date,6,2);
    my %yyyymmdd=(
        "01"=>31,
        "03"=>31,
        "05"=>31,
        "07"=>31,
        "08"=>31,
        "10"=>31,
        "12"=>31,
        "04"=>30,
        "06"=>30,
        "09"=>30,
        "11"=>30
    );
    #判断闰年
    if(($yyyy % 400 == 0) or ($yyyy % 4 == 0 and $yyyy % 100 != 0)){
        $yyyymmdd{"02"} = 29;
    }else{
        $yyyymmdd{"02"} = 28;
    }

    if(!exists $yyyymmdd{$mm}){return 0;}   #月份必须在[01,12]区间
    my $maxDD = $yyyymmdd{$mm};             #该月份最大的日期

    if($dd >0 and $dd <= $maxDD){
        return 1;
    }else{
        return 0;
    }
}

#获得当前日期以及时间，格式如：2011-01-02 12:13:50
sub getCurDateTime
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";

   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);

   $current = "${year}-${mon}-${mday} ${hour}:${min}:${sec}";

   return $current;
}

#获得当前八位日期
sub getCurDate{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());

   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);

   return "${year}${mon}${mday}";
}

#获取当前八位时间
sub getCurTime{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());

   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);

   return "${hour}${min}${sec}";
}

#获取当前格式化后的时间（HH:mm:ss）
sub getCurFormatTime{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());

   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);

   return "${hour}:${min}:${sec}";
}

#初始化数据库配置信息
sub initDBCfg{
    unless(%db_cfg){
        tie %db_cfg,'Config::IniFiles',(-file => File::Spec->catfile($ENV{"AUTO_HOME"},'etc','db_cfg.ini'));
    }
}
#根据节点获得配置文件中的数据库用户名和密码并返回数组
sub getUserAndPwd{
    initDBCfg();
    my $tag = $_[0];
    my $usr = $db_cfg{$tag}{usr};
    my $pwd = $db_cfg{$tag}{pwd};
    $pwd = CharCrypt::rc4Decrypt($pwd);
    return ($usr,$pwd);
}

#根据driver，username，passwd获取数据库连接
sub getDbh{
    my ($driver, $username, $passwd) = @_;
    DBI->trace(0);
    my $dbh;
    #retry 3 times at intervals of 10 seconds
    my $sleeptime = 5;
    my $retry = 3;
    for(my $i = 0;$i<$retry;$i++){
        $dbh = DBI->connect("$driver", $username, $passwd,
                          { AutoCommit => 1 ###  AutoCommit
#                           PrintError => 0, ### Don't report errors via warn()
#                           RaiseError => 0  ### Don't report errors via die()
                          } ) or warning "Can't connect to '$driver': $DBI::errstr.";
        if($dbh){
            return $dbh
        }else{
            if($i == ($retry-1)){return undef;}
            warning "reconnect '$driver' after $sleeptime seconds.";
            sleep($sleeptime);
        }
    }
}

#根据tag获取数据库连接
sub getDbhByTag{
    initDBCfg();
    my $tag = $_[0];
    my $driver = $db_cfg{$tag}{driver};
    my ($user,$pwd) = getUserAndPwd($tag);
    return getDbh($driver,$user,$pwd);
}

#获得数据库中系统管理参数配置
sub getSysAdminConfig{
    my $dbh = getDbhByTag('ECIF_TD');
    my $db = getCfg('ENV','ECIFSYSADMIN');
    my $sql = "select tag,config_key,config_value,is_encoded from $db.sys_config";
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my %cofig;
    while ( my @ref = $sth->fetchrow_array() ) {
        my ($tag,$key,$value,$is_cncoded) = @ref;
        if($is_cncoded eq '1'){$value = CharCrypt::rc4Decrypt($value);}
        $cofig{$tag}{$key} = $value;
    }
    $dbh->disconnect();
    return %cofig;
}

#插入事件日志.参数:系统名称(三位),事件级别(一位 L低 M中 H高),事件描述
sub insertEventLog{
    my ( $system, $severity, $desc ) = @_;
    $system =~ s/\'/''/g;
    $severity =~ s/\'/''/g;
    $desc =~ s/\'/''/g;
    my $retry = 5; #eventID 有可能重复，尝试插入 5次
    for(my $i = 0;$i< $retry;$i++){
        unless(__insertEventLog( $system, $severity, $desc )){
            next;
        }else{
            last;
        }
    }
    if($ETL::EVENT_LOG_FLAG eq '1' && (uc($severity) eq 'H' || uc($severity) eq 'M')){
        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());

        $hour = sprintf("%02d", $hour);
        $min  = sprintf("%02d", $min);
        $sec  = sprintf("%02d", $sec);
        my $level = 0;
        if(uc($severity) eq 'H'){
            $level = 2;
        }elsif(uc($severity) eq 'M'){
            $level = 1;
        }
#        ETL::printETLEvent("${hour}:${min}:${sec}",$desc,$level);
   }
}
#内部使用
sub __insertEventLog {
    my ( $system, $severity, $desc ) = @_;
    my $dbh = getDbhByTag('AUTOMATION');
    my $curtime = getCurDateTime();
    my $logtime = $curtime;
    $curtime =~ s/\D//g;            #去掉日期格式中非数字部分
    my ($seconds,$miscroseconds)  =Time::HiRes::gettimeofday();
    $miscroseconds = sprintf("%06d",$miscroseconds);
    my $eventid = "${curtime}${miscroseconds}";       #$eventid 长度20 年月日时分秒+六位微妙
    my $petl=getCfg('ENV','PETL');
    my $sqlText = "INSERT INTO $petl.ETL_Event
                (
                    EventID,
                    EventStatus,
                    Severity,
                    Description,
                    LogTime,
                    CloseTime
                )
                VALUES
                (
                    '$eventid',
                    'O',
                    '$severity',
                    '$desc',
                    '$logtime',
                    NULL
                );";
    debug($sqlText);
    my $sth = $dbh->prepare($sqlText) or error("prepare SQL error:$DBI::errstr"),return 0;
    $sth->execute() or error("execute SQL error:$DBI::errstr"),return 0;
    $dbh->commit() or error("commit error:$DBI::errstr"),return 0;
    $sth->finish();
    $dbh->disconnect;
    return 1;
}

#去掉字符串前后的空格
sub trim{
    my $str = $_[0];
    unless(defined $str){return undef}
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    return $str;
}

#给EOD发信号。返回值 0：失败，1：成功
sub sendEODMsg{
    my ($msg) = @_;
    my $cps_webservice_proxy = getCfg('CPS_SERVICE','cps_webservice_proxy');
    my $cps_webservice_uri   = getCfg('CPS_SERVICE','cps_webservice_uri');
    unless($cps_webservice_proxy || $cps_webservice_uri){
        error("undefine cps_webservice_proxy or  cps_webservice_uri");
        return 0;
    }
    my $soap =
      SOAP::Lite->proxy(
        "$cps_webservice_proxy")
      ->uri("$cps_webservice_uri") or do{error($!);return 0};

    my $result = $soap->ackEvent(
        SOAP::Data->name( 'eventName' => "$msg" )
    ) or do{error($!);return 0};

    if($result->fault){
        error("EOD Msg sended failed, faultstring\t:", Encode::CN::encode( "gb2312", $result->faultstring));
        return 0;
    }else{
        my $tmp    = $result->result();
        my %result = %$tmp;
        my $domain = $result{domain} || '';
        my $message = $result{message} || '';
        my $type = $result{type} || '';
        my $str = "domain=$domain,message=$message,type=$type";
        $str = Encode::CN::encode( "gb2312",$str);
        info("EOD Msg send result-->$str");
        if($result{type} eq 'S'){
            return 1;
        }
        return 0;
    }
}

#给MDS发信号。返回值 0：失败，1：成功
sub sendMDSMsg{
    my ($msg, $txdate) = @_;
    my $webservice_proxy = getCfg('MDS_SERVICE','webservice_proxy');
    my $webservice_uri   = getCfg('MDS_SERVICE','webservice_uri');
    unless($webservice_proxy || $webservice_uri){
        error("undefine webservice_proxy or webservice_uri");
        return 0;
    }
    my $soap =
      SOAP::Lite->proxy(
        "$webservice_proxy")
      ->uri("$webservice_uri") or do{error($!);return 0};

    my $result = $soap->mdsService(
        SOAP::Data->name( args0 => $msg),
        SOAP::Data->name( args1 => 'S'),
        SOAP::Data->name( args2 => $txdate)
    ) or do{error($!);return 0};

    if($result->fault){
        error("MDS Msg sended failed, faultstring\t:", Encode::CN::encode( "gb2312", $result->faultstring));
        return 0;
    }else{
        my $tmp    = $result->result();
        my %result = %$tmp;
        my $code = $result{mesBean}{code} || '';
        my $mes  = Encode::CN::encode('gb2312',$result{mesBean}{message}) || '';
        my $type = $result{mesBean}{type} || '';
        if($type eq 'S'){
            info("sendMDSMsg success:TYPE[$type], Code[$code], Message[$mes]");
            return 1;
        }else{
            error("sendMDSMsg failed:TYPE[$type], Code[$code], Message[$mes]");
        }
        return 0;
    }
}

#给SMS发信号。返回值 0：失败，1：成功
sub sendSMSMsg{
    my ($server,$type,$AddressType,$MsgClassId,$SMSId,$FileName,$MsgFormat,$AddCount) = @_;
    my $address;
#   type 区分是对账单，还是sms  0:对账单，1：sms
    if($type eq '1'){
        $address = "/msgApp/services/SMSC210Service";
    }else{
        $address = "/msgservice/SMSC210ServiceService";
    }
    my %reqHead = (
        ADDRESSTYPE => SOAP::Data->type('string')->value($AddressType),
        MSGCLASSID  => SOAP::Data->type('string')->value($MsgClassId),
        SMSID       => SOAP::Data->type('string')->value($SMSId),
        FILENAME    => SOAP::Data->type('string')->value($FileName),
        MSGFORMAT   => SOAP::Data->type('string')->value($MsgFormat),
        ADDCOUNT    => SOAP::Data->type('string')->value($AddCount),
        TRCD        => SOAP::Data->type('string')->value("C210"),
        UTNO        => SOAP::Data->type('string')->value("ECIF"),
        SBNO        => SOAP::Data->type('string')->value("ECIF"),
        TELR        => SOAP::Data->type('string')->value("000000"),
        APPID       => SOAP::Data->type('string')->value("ECIF00")
    );
    my $soap =
      SOAP::Lite->proxy(
        "http://$server$address")
      ->uri('http://cxt.com/ws/service/') or error($!);

    my $result = $soap->c210Service(
        SOAP::Data->name( 'req' => \%reqHead )
    ) or error($!);

    if($result->fault){
        error("SMS Msg sended failed, faultstring\t:", Encode::CN::encode( "gb2312", $result->faultstring));
        return 0;
    }else{
        my $tmp    = $result->result();
        my %result = %$tmp;
        my $MGID = $result{MGID} || '';
        my $code = $result{RETURNCODE}{code} || '';
        my $domain = $result{RETURNCODE}{domain} || '';
        my $message = $result{RETURNCODE}{message} || '';
        my $type = $result{RETURNCODE}{type} || '';
        my $RTXT = $result{RTXT} || '';
        my $SBNO = $result{SBNO} || '';
        my $TELR = $result{TELR} || '';
        my $TRDT = $result{TRDT} || '';
        my $TRTM = $result{TRTM} || '';
        my $str = "MGID=$MGID,code=$code,domain=$domain,message=$message,type=$type,RTXT=$RTXT,SBNO=$SBNO,TELR=$TELR,TRDT=$TRDT,TRTM=$TRTM";
        $str = Encode::CN::encode( "gb2312",$str);
        info("SMS Msg send result-->$str");
        if($type eq 'S'){
            return 1;
        }
        return 0;
    }
}

#ftp上传完成信号.返回值 0：失败，1：成功
sub sendFTPMsg{
    my ($server,$msg) = @_;
    my $soap =
      SOAP::Lite->proxy(
        "$server")
      ->uri('http://service.print.hiaward.com/') or error($!);

    my $result = $soap->updateCache(
        SOAP::Data->name( 'filename' => "$msg" )
    ) or error($!);

    if($result->fault){
        error("FTP Msg sended failed, faultstring\t:", Encode::CN::encode( "gb2312", $result->faultstring));
        return 0;
    }else{
        my $tmp    = $result->result();
        my %result = %$tmp;
        my $message = $result{message} || '';
        my $type = $result{type} || '';
        my $code = $result{code} || '';
        my $str = "message=$message,type=$type,code=$code";
        $str = Encode::CN::encode( "gb2312",$str);
        info("EOD Msg send result-->$str");
        if($result{type} eq 'S'){
            return 1;
        }
        return 0;
    }
}

#获取给其他系统发信号的标志#
sub getMsgSendFlag{
    return getSysCfg('EOD_CFG','send_message_flag');
}

#创建深层目录，成功返回1失败返回0
sub makePath{
    my $path = shift;
    eval{
        mkpath("$path");
    };
    if($@){
        return 0;
    }else{
        return 1;
    }
}

#转换文件编码 参数格式 "-f utf8 -t cp936 sourct_file>dest_file"
sub transFileEncode{
    my $para = $_[0];
    return ECIF->iconv($para);
}

#传入一个lock文件，判断对应的服务是否是单例服务
sub checkInstance {
    my $lock = shift;
    if ( -e  $lock) {
        open( FL, ">>$lock" ) or return 0;
        flock( FL, LOCK_EX | LOCK_NB) or return 0;
        close FL;
    }
    my $path = dirname($lock);
    if(!-e $path){
        makePath($path) or die "cann not makepath $path";
    }
    open( FL, ">$lock" ) or return 0;
    FL->autoflush(1);
    flock( FL, LOCK_EX | LOCK_NB) or return 0;
    print FL $$;
    FL->autoflush(1);
    flock( FL, LOCK_UN );
    flock( FL, LOCK_SH );
    $SIG{'KILL'} = $SIG{'TERM'} = $SIG{'QUIT'} = $SIG{'INT'}  = sub{
        my ($signal) = @_;
        warning("Stop by signal $signal");
        close FL;
        unlink($lock) or die "$!";
        exit;
    };
    my $client_ip = ECIF::getClientIp();
    if($client_ip ne ''){
        warning("start by remote user $client_ip\n");
    }
    return \*FL;
}

#关闭实例(即释放文件锁)
sub shutdownInstance {
    my $FL = shift;
    close $FL;
}

#检查回单系统是否就绪
sub checkBrmsStat{
    my($table_name,$tx_date) = @_;
    my $yesterday = dateCalc($tx_date,-1);
    #check yesterday's status
    my $dbh = getDbhByTag('BRMS_DB2');
    my $sql_text = "SELECT JOB_STATUS
                    FROM PMART.ECIF_JOB_STATUS
                    WHERE BIZ_DT = TO_DATE('$yesterday','YYYYMMDD')
                        AND JOB_NAME='$table_name';";
    my $sth = $dbh->prepare($sql_text) or fatal("$DBI::errstr");
    #check it every minute.if not ready,warn and wait.throw alert every 30 minutes.
    my $wait_time = 0;
    my @stat_row;
    while ( 1 ) {
        $sth->execute() or fatal("query checkstatus failed:$DBI::errstr") ;
        @stat_row = $sth->fetchrow_array();
        if ( @stat_row ) {
            #0未处理/1处理中/2完成/3失败
            if ( $stat_row[0] eq '3' ) {
                $sth->finish();
                $dbh->disconnect();
                fatal("The process of BRMS table $table_name is failed yesterday.We cannot replace it,script exit.");
            }
            elsif ( $stat_row[0] eq '2' ) {
                info("The BRMS table $table_name is ready.We will replace it.");
                last;
            }
            else {
                #alert once and exit after 30 minutes if not fixed.
                if ( $wait_time == 0 ) {
                    insertEventLog( "TDB", "H", "[TDB], Please contact BRMS to process table [$table_name] in 30 minutes.");
                    warning("Wait for BRMS to process the table $table_name.");
                }
                elsif ( $wait_time == 30 ) {
                    $sth->finish();
                    $dbh->disconnect();
                    fatal("Program terminate because BRMS status table $table_name is not ready.");
                }
                sleep 60;
                $wait_time++;
            }
        }
        else {
            warning("Can't find the status of job $table_name in PMART.ECIF_JOB_STATUS.");
            last;
        }
    }
    $sth->finish();
    $dbh->disconnect();
    info("check BRMS status pass.");
    return 0;
}

#插入回单通知
sub insertBrmsMessage{
    my($table_name,$tx_date) = @_;
    #init ivc flag. WARNING: hard code here.
    my $is_ivc;
    if ( $table_name eq 'IVC_CMBC_BANK_COM_ITL_ORG' or
         $table_name eq 'IVC_CRD_ACT_REAL' ) {
        $is_ivc = 'N';
    }
    else {
        $is_ivc = 'Y';
    }
    #start to insert the message.
    my $dbh = getDbhByTag('BRMS_DB2');
    $dbh->do( "DELETE FROM PMART.ECIF_JOB_STATUS
               WHERE  BIZ_DT = TO_DATE('$tx_date','YYYYMMDD')
                 AND  JOB_NAME='$table_name'");
    $dbh->do( "INSERT INTO PMART.ECIF_JOB_STATUS
               VALUES ('$table_name'
                      ,TO_DATE('$tx_date','YYYYMMDD')
                      ,'0'
                      ,CURRENT TIMESTAMP
                      ,null
                      ,'$is_ivc')" ) or fatal("$DBI::errstr");
    $dbh->disconnect();
    info("we have sent the ready message of job $table_name to BRMS.");
    return 0;
}

#检查回单系统是否就绪 修改为只检查当天是否正在处理，旧程序暂时保留
sub checkBrmsMessage{
    my($table_name,$tx_date) = @_;
    #check today's status
    my $dbh = getDbhByTag('BRMS_DB2');
    my $sql_text = "SELECT JOB_STATUS
                    FROM PMART.ECIF_JOB_STATUS
                    WHERE BIZ_DT = TO_DATE('$tx_date','YYYYMMDD')
                        AND JOB_NAME='$table_name';";
    my $sth = $dbh->prepare($sql_text) or fatal("$DBI::errstr");
    #check it every minute.if not ready,warn and wait.throw alert every 30 minutes.
    my $wait_time = 0;
    my @stat_row;
    while ( 1 ) {
        $sth->execute() or fatal("query checkstatus failed:$DBI::errstr") ;
        @stat_row = $sth->fetchrow_array();
        if ( @stat_row ) {
            #0未处理/1处理中/2完成/3失败
            if ( $stat_row[0] eq '2' or $stat_row[0] eq '3' ) {
                insertEventLog( "TDB", "M", "[TDB], Please ask BRMS to process table [$table_name] again,we've rerun it.");
                warning("The BRMS table $table_name have been processed.but we will replace it.");
                last;
            }
            elsif ( $stat_row[0] eq '1' ) {
                #alert once and exit after 30 minutes if not fixed.
                if ( $wait_time == 0 ) {
                    insertEventLog( "TDB", "M", "[TDB], BRMS is processing table [$table_name], we'll wait for 30 minutes.");
                    warning("Wait for BRMS to process the table $table_name.");
                }
                elsif ( $wait_time >= 30 ) {
                    $sth->finish();
                    $dbh->disconnect();
                    fatal("Program terminate because BRMS is processing today's table $table_name .");
                }
                sleep 60;
                $wait_time++;
            }
            else {
                #just replace
                last;
            }
        }
        else {
            debug("Can not find the status of job $table_name in PMART.ECIF_JOB_STATUS. First time to run.");
            last;
        }
    }
    $sth->finish();
    $dbh->disconnect();
    info("check BRMS status pass.");
    return 0;
}

#删除回单通知 如果重跑，应该在下发数据前就删除，避免回单开始处理
sub deleteBrmsMessage{
    my($table_name,$tx_date) = @_;
    #start to insert the message.
    my $dbh = getDbhByTag('BRMS_DB2');
    $dbh->do( "DELETE FROM PMART.ECIF_JOB_STATUS
               WHERE  BIZ_DT = TO_DATE('$tx_date','YYYYMMDD')
                 AND  JOB_NAME='$table_name'");
    $dbh->disconnect();
    return 0;
}

#根据db_cfg.ini里的标签名获取数据库名(或别名)
sub getDbNameByTag{
    my $tag = shift;
    tie %db_cfg,'Config::IniFiles',(-file => File::Spec->catfile($ENV{"AUTO_HOME"},'etc','db_cfg.ini'));
    my $driver = $db_cfg{$tag}{driver};
    unless(defined $driver){
        warning("no tag match $tag");
        return undef;
    }
    my @para = split(/:/,$driver);
    return $para[2];
}

#判断TD的连接是否是活跃的
sub isActive{
    my $dbh = shift;
    unless($dbh){return 0}
    my $sql = "select 123,date";
    my $flag = 0;
    eval{
        my $ary_ref = $dbh->selectall_arrayref($sql);
        if($ary_ref->[0]->[0] && $ary_ref->[0]->[0] == 123){
            $flag =  1;
        }
    };
    if($@){
        return 0;
    }
    if($flag){
        return 1;
    }else{
        return 0;
    }
}
#压缩文件
sub compressFile{
    my ($srcFile,$descFile) = @_;
    return ECIF->__compressFile($srcFile,$descFile);
}

sub getFileMD5{
    my $file = $_[0];
    if(!-f $file){
        error("$file not found");return "";
    }
    my $md5 = Digest::MD5->new();
    open FILE,$file;
    binmode(FILE);
    $md5->addfile(*FILE);
    close FILE;
    return $md5->hexdigest();
}

#获得上月月末
sub getLastMonthEndDay{
    my $date = shift;
    my $year = substr($date,0,4);
    my $month = substr($date,4,2);
    if($month eq '01'){
        return ($year-1).'1231';
    }elsif($month <= 10){
        return $year.'0'.($month-1).getMaxDay($year,'0'.($month-1));
    }else{
        return $year.($month-1).getMaxDay($year.($month-1));
    }
}

sub getMaxDay{
    my $date = shift;
    my $yyyy = substr($date,0,4);
    my $mm = substr($date,4,2);
    if(length($mm)==1){
        $mm = "0".$mm;
    }
    my %yyyymmdd=(
        "01"=>31,
        "03"=>31,
        "05"=>31,
        "07"=>31,
        "08"=>31,
        "10"=>31,
        "12"=>31,
        "04"=>30,
        "06"=>30,
        "09"=>30,
        "11"=>30
    );
    #判断闰年
    if(($yyyy % 400 == 0) or ($yyyy % 4 == 0 and $yyyy % 100 != 0)){
        $yyyymmdd{"02"} = 29;
    }else{
        $yyyymmdd{"02"} = 28;
    }
    return $yyyymmdd{$mm};
}

sub getSecondsByTimestmp{
    my $timestmp = shift;
    if(defined $timestmp && $timestmp =~ m/(^\d{4})\-(\d{2})\-(\d{2}) (\d{2}):(\d{2}):(\d{2})/){
        return timelocal($6,$5,$4,$3,$2-1,$1);
    }else{
        return '';
    }
}

###############################################################################
# OS dependent function section
###############################################################################
sub getClientIp{
    return ECIF->_getClientIp();
}


1;

__END__
