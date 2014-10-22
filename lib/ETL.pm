#!/usr/bin/perl
#########################################################################################################
#Function:              ETL ����ģ��
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
my %ecif_local_cfg;                 #�ļ��е�������Ϣ
my %ecif_sys_cfg;                   #���ݿ���ϵͳ�����������
my %db_cfg;                         #���ݿ�����������Ϣ
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
    #�����Ӻ����б�
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
#��ȡ������Ϣ������ļ��е����ú����ݿ��е����ó�ͻ��˭Ϊ׼�����ۣ�δʵ�֣�
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

#��ȡ�����ļ��е�����
sub getCfg{
    my ($tag,$key) = @_;
#   ��ȡ�����صķ�ʽ
    unless(%ecif_local_cfg){
        tie %ecif_local_cfg,'Config::IniFiles',(-file => File::Spec->catfile($ENV{"AUTO_HOME"},'config','ecif_cfg.ini'));
    }
#   ���û�и�key�����tag�����е�������hash����ʽ����
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

#��ȡ���ݿ�ϵͳ�����������
sub getSysCfg{
    my ($tag,$key) = @_;
#   ��ȡ�����صķ�ʽ
    unless(%ecif_sys_cfg){
        %ecif_sys_cfg = getSysAdminConfig();
    }
    #   ���û�и�key�����tag�����е�������hash����ʽ����
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

#���ڼ���
#���� $date   ����Ĳ�������
#���� $offset ��������ڵ�ƫ������������ʾ�Ժ�����ڣ�������ʾ��ǰ������
sub dateCalc{
    my ( $date, $offset ) = @_;
    unless(isValidDate($date)){return undef;} #����У��ο����ڵ���Ч��
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

    #��ʽ��ΪYYYYMMDD����ʽ
    my $yyyymmdd = sprintf( "%04d%02d%02d", ${year}, ${month}, ${mday} );
    return $yyyymmdd;
}

#У�� yyyymmdd ���ڸ�ʽ����Ч�� ��Ч���� 1 ��Ч���� 0
sub isValidDate{
    my $date = $_[0];
    #���ȱ���Ϊ8�Ұ�λ���ڱ���Ϊ����
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
    #�ж�����
    if(($yyyy % 400 == 0) or ($yyyy % 4 == 0 and $yyyy % 100 != 0)){
        $yyyymmdd{"02"} = 29;
    }else{
        $yyyymmdd{"02"} = 28;
    }

    if(!exists $yyyymmdd{$mm}){return 0;}   #�·ݱ�����[01,12]����
    my $maxDD = $yyyymmdd{$mm};             #���·���������

    if($dd >0 and $dd <= $maxDD){
        return 1;
    }else{
        return 0;
    }
}

#��õ�ǰ�����Լ�ʱ�䣬��ʽ�磺2011-01-02 12:13:50
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

#��õ�ǰ��λ����
sub getCurDate{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());

   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);

   return "${year}${mon}${mday}";
}

#��ȡ��ǰ��λʱ��
sub getCurTime{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());

   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);

   return "${hour}${min}${sec}";
}

#��ȡ��ǰ��ʽ�����ʱ�䣨HH:mm:ss��
sub getCurFormatTime{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());

   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);

   return "${hour}:${min}:${sec}";
}

#��ʼ�����ݿ�������Ϣ
sub initDBCfg{
    unless(%db_cfg){
        tie %db_cfg,'Config::IniFiles',(-file => File::Spec->catfile($ENV{"AUTO_HOME"},'etc','db_cfg.ini'));
    }
}
#���ݽڵ��������ļ��е����ݿ��û��������벢��������
sub getUserAndPwd{
    initDBCfg();
    my $tag = $_[0];
    my $usr = $db_cfg{$tag}{usr};
    my $pwd = $db_cfg{$tag}{pwd};
    $pwd = CharCrypt::rc4Decrypt($pwd);
    return ($usr,$pwd);
}

#����driver��username��passwd��ȡ���ݿ�����
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

#����tag��ȡ���ݿ�����
sub getDbhByTag{
    initDBCfg();
    my $tag = $_[0];
    my $driver = $db_cfg{$tag}{driver};
    my ($user,$pwd) = getUserAndPwd($tag);
    return getDbh($driver,$user,$pwd);
}

#������ݿ���ϵͳ�����������
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

#�����¼���־.����:ϵͳ����(��λ),�¼�����(һλ L�� M�� H��),�¼�����
sub insertEventLog{
    my ( $system, $severity, $desc ) = @_;
    $system =~ s/\'/''/g;
    $severity =~ s/\'/''/g;
    $desc =~ s/\'/''/g;
    my $retry = 5; #eventID �п����ظ������Բ��� 5��
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
#�ڲ�ʹ��
sub __insertEventLog {
    my ( $system, $severity, $desc ) = @_;
    my $dbh = getDbhByTag('AUTOMATION');
    my $curtime = getCurDateTime();
    my $logtime = $curtime;
    $curtime =~ s/\D//g;            #ȥ�����ڸ�ʽ�з����ֲ���
    my ($seconds,$miscroseconds)  =Time::HiRes::gettimeofday();
    $miscroseconds = sprintf("%06d",$miscroseconds);
    my $eventid = "${curtime}${miscroseconds}";       #$eventid ����20 ������ʱ����+��λ΢��
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

#ȥ���ַ���ǰ��Ŀո�
sub trim{
    my $str = $_[0];
    unless(defined $str){return undef}
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    return $str;
}

#��EOD���źš�����ֵ 0��ʧ�ܣ�1���ɹ�
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

#��MDS���źš�����ֵ 0��ʧ�ܣ�1���ɹ�
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

#��SMS���źš�����ֵ 0��ʧ�ܣ�1���ɹ�
sub sendSMSMsg{
    my ($server,$type,$AddressType,$MsgClassId,$SMSId,$FileName,$MsgFormat,$AddCount) = @_;
    my $address;
#   type �����Ƕ��˵�������sms  0:���˵���1��sms
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

#ftp�ϴ�����ź�.����ֵ 0��ʧ�ܣ�1���ɹ�
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

#��ȡ������ϵͳ���źŵı�־#
sub getMsgSendFlag{
    return getSysCfg('EOD_CFG','send_message_flag');
}

#�������Ŀ¼���ɹ�����1ʧ�ܷ���0
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

#ת���ļ����� ������ʽ "-f utf8 -t cp936 sourct_file>dest_file"
sub transFileEncode{
    my $para = $_[0];
    return ECIF->iconv($para);
}

#����һ��lock�ļ����ж϶�Ӧ�ķ����Ƿ��ǵ�������
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

#�ر�ʵ��(���ͷ��ļ���)
sub shutdownInstance {
    my $FL = shift;
    close $FL;
}

#���ص�ϵͳ�Ƿ����
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
            #0δ����/1������/2���/3ʧ��
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

#����ص�֪ͨ
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

#���ص�ϵͳ�Ƿ���� �޸�Ϊֻ��鵱���Ƿ����ڴ����ɳ�����ʱ����
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
            #0δ����/1������/2���/3ʧ��
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

#ɾ���ص�֪ͨ ������ܣ�Ӧ�����·�����ǰ��ɾ��������ص���ʼ����
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

#����db_cfg.ini��ı�ǩ����ȡ���ݿ���(�����)
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

#�ж�TD�������Ƿ��ǻ�Ծ��
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
#ѹ���ļ�
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

#���������ĩ
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
    #�ж�����
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
