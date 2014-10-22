#!/usr/bin/perl
#########################################################################################################
#Function:              ��������ģ��
#Author:                WuGang
#Date Time:
#History:
#Copyright              2011 VanceInfo All Rights Reserved.
#########################################################################################################
package ETLDate;
use Time::Local;
use Logger;

$ETLDate::TODAY=undef;
$ETLDate::MONTH=undef;
$ETLDate::MAX_DAY=undef;                  #�����������
$ETLDate::LAST_DAY=undef;                 #����
$ETLDate::MONTH_START=undef;              #�³�
$ETLDate::MONTH_END=undef;                #��ĩ
$ETLDate::LAST_MONTH_START=undef;         #�����³�
$ETLDate::LAST_MONTH_END=undef;           #������ĩ
$ETLDate::LAST_2_MONTH_START=undef;       #�����³�
$ETLDate::LAST_2_MONTH_END=undef;         #������ĩ
$ETLDate::LAST_3_MONTH_START=undef;       #�������³�
$ETLDate::LAST_3_MONTH_END=undef;         #��������ĩ
$ETLDate::YEAR_START=undef;               #�������
$ETLDate::YEAR_END=undef;                 #������ĩ
$ETLDate::LAST_YEAR_END=undef;            #ȥ����ĩ
$ETLDate::LAST_YEAR_SAME_DATE=undef;      #ȥ��ͬ��
$ETLDate::LAST_YEAR_NEXT_DATE=undef;      #ȥ��ͬ��+1��
$ETLDate::LAST_QUAR_START=undef;          #�ϼ�����
$ETLDate::LAST_QUAR_END=undef;            #�ϼ���ĩ
$ETLDate::SEASON_START=undef;             #������
$ETLDate::SEASON_END=undef;               #����ĩ
$ETLDate::CUR_TENDAYS_START=undef;        #��Ѯ��
$ETLDate::CUR_TENDAYS_END=undef;          #��Ѯĩ

$ETLDate::LAST_MONTH_NEXT_DAY=undef;      #��ǰ���ڼ�һ���� + 1
$ETLDate::LAST_3_MONTH_NEXT_DAY=undef;    #��ǰ���ڼ������� + 1
$ETLDate::TEND_FIRST_DAY=undef;           #��ǰ���ڼ�ʮ�� + 1

$ETLDate::NULL_DATE = "19000102";
$ETLDate::MIN_DATE  = "19000101";
$ETLDate::MAX_DATE  = "30001231";

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

#���� 1 $tx_date          �ο�����
#���� 2 $cast_date_flag   �Ƿ�ת�����ڸ�ʽ��־��1��ת��
sub init {
    my ($tx_date,$cast_date_flag) = @_;
    if ( !isValidDate($tx_date) ) {
        fatal("$tx_date is not a valid date");
    }
#   init ǰ ���������
    $ETLDate::TODAY=undef;
    $ETLDate::MONTH=undef;
    $ETLDate::MAX_DAY=undef;                  #�����������
    $ETLDate::LAST_DAY=undef;                 #����
    $ETLDate::MONTH_START=undef;              #�³�
    $ETLDate::MONTH_END=undef;                #��ĩ
    $ETLDate::LAST_MONTH_START=undef;         #�����³�
    $ETLDate::LAST_MONTH_END=undef;           #������ĩ
    $ETLDate::LAST_2_MONTH_START=undef;       #�����³�
    $ETLDate::LAST_2_MONTH_END=undef;         #������ĩ
    $ETLDate::LAST_3_MONTH_START=undef;       #�������³�
    $ETLDate::LAST_3_MONTH_END=undef;         #��������ĩ
    $ETLDate::YEAR_START=undef;               #�������
    $ETLDate::YEAR_END=undef;                 #������ĩ
    $ETLDate::LAST_YEAR_END=undef;            #ȥ����ĩ
    $ETLDate::LAST_YEAR_SAME_DATE=undef;      #ȥ��ͬ��
    $ETLDate::LAST_YEAR_NEXT_DATE=undef;      #ȥ��ͬ��+1��
    $ETLDate::LAST_QUAR_START=undef;          #�ϼ�����
    $ETLDate::LAST_QUAR_END=undef;            #�ϼ���ĩ
    $ETLDate::SEASON_START=undef;             #������
    $ETLDate::SEASON_END=undef;               #����ĩ
    $ETLDate::CUR_TENDAYS_START=undef;        #��Ѯ��
    $ETLDate::CUR_TENDAYS_END=undef;          #��Ѯĩ

    $ETLDate::LAST_MONTH_NEXT_DAY=undef;      #��ǰ���ڼ�һ���� + 1
    $ETLDate::LAST_3_MONTH_NEXT_DAY=undef;    #��ǰ���ڼ������� + 1
    $ETLDate::TEND_FIRST_DAY=undef;           #��ǰ���ڼ�ʮ�� + 1

    $ETLDate::NULL_DATE = "19000102";
    $ETLDate::MIN_DATE  = "19000101";
    $ETLDate::MAX_DATE  = "30001231";

    my $year  = substr( $tx_date, 0, 4 );
    my $month = substr( $tx_date, 4, 2 );
    my $day   = substr( $tx_date, 6, 2 );
    my $pre_year  = getPreYear($year);
    my $pre_month = getPreMonth($month);

    $ETLDate::TODAY = $tx_date;
    $ETLDate::MONTH = "$year$month";

    #�����������
    $ETLDate::MAX_DAY = getMaxDay($year,$month);
    #   ȥ����ĩ
    $ETLDate::LAST_YEAR_END = $pre_year . "1231";

    #�����³���������ĩ
    if ( $month eq '01' ) {
        $ETLDate::LAST_MONTH_START = $pre_year.$pre_month."01";
        $ETLDate::LAST_MONTH_END = $ETLDate::LAST_YEAR_END;
    }
    else {
        $ETLDate::LAST_MONTH_START = $year.$pre_month. "01";
        $ETLDate::LAST_MONTH_END   = $year.$pre_month.getMaxDay($year,$pre_month);
    }
    #��ǰ���ڼ�һ���� + 1
    my $last_month_max_day = getMaxDay(substr($ETLDate::LAST_MONTH_START,0,4),substr($ETLDate::LAST_MONTH_START,4,2));
    if(($day+1)>$last_month_max_day){
        $ETLDate::LAST_MONTH_NEXT_DAY = "$year$month".'01';
    }else{
        $ETLDate::LAST_MONTH_NEXT_DAY = dateCalc(substr($ETLDate::LAST_MONTH_START,0,6)."$day",1);
    }


    #   �������
    $ETLDate::YEAR_START = $year . '0101';

    #   ȥ��ͬ��
    if(isValidDate($pre_year.$month.$day)){
        $ETLDate::LAST_YEAR_SAME_DATE = $pre_year.$month.$day;
    }else{
        $ETLDate::LAST_YEAR_SAME_DATE = $pre_year.$month.getMaxDay($pre_year,$month);
    }
    $ETLDate::LAST_YEAR_NEXT_DATE = dateCalc($ETLDate::LAST_YEAR_SAME_DATE,1);

    #   �³�
    $ETLDate::MONTH_START = $year . $month . '01';

    #   ��ĩ
    $ETLDate::MONTH_END = $year.$month.getMaxDay($year,$month);

    #������ĩ
    $ETLDate::YEAR_END = $year."1231";
    #�ϼ��������ϼ���ĩ��������������ĩ
    if($month<=3){
        $ETLDate::LAST_QUAR_START = $pre_year."1001";
        $ETLDate::LAST_QUAR_END = $pre_year."1231";
        $ETLDate::SEASON_START = $year."0101";
        $ETLDate::SEASON_END = $year."0331";
    }elsif($month<=6){
        $ETLDate::LAST_QUAR_START = ($year)."0101";
        $ETLDate::LAST_QUAR_END = ($year)."0331";
        $ETLDate::SEASON_START = $year."0401";
        $ETLDate::SEASON_END = $year."0630";
    }elsif($month<=9){
        $ETLDate::LAST_QUAR_START = ($year)."0401";
        $ETLDate::LAST_QUAR_END = ($year)."0630";
        $ETLDate::SEASON_START = $year."0701";
        $ETLDate::SEASON_END = $year."0930";
    }elsif($month<=12){
        $ETLDate::LAST_QUAR_START = ($year)."0701";
        $ETLDate::LAST_QUAR_END = ($year)."0930";
        $ETLDate::SEASON_START = $year."1001";
        $ETLDate::SEASON_END = $year."1231";
    }
    #�����³���������ĩ
    if($month eq '01'){
        $ETLDate::LAST_2_MONTH_START = $pre_year."1101";
        $ETLDate::LAST_2_MONTH_END = $pre_year."1130";
    }elsif($month eq '02'){
        $ETLDate::LAST_2_MONTH_START = $pre_year."1201";
        $ETLDate::LAST_2_MONTH_END = $pre_year."1231";
    }else{
        my $tmp_month = getPreMonth($pre_month);
        $ETLDate::LAST_2_MONTH_START = $year.$tmp_month."01";
        $ETLDate::LAST_2_MONTH_END = $year.$tmp_month.getMaxDay($year,$tmp_month);
    }
    #�������³�����������ĩ
    my $last_2_month = substr($ETLDate::LAST_2_MONTH_START,4,2);
    if($last_2_month eq '01'){
        $ETLDate::LAST_3_MONTH_START = $pre_year."1101";
        $ETLDate::LAST_3_MONTH_END = $pre_year."1130";
    }elsif($last_2_month eq '02'){
        $ETLDate::LAST_3_MONTH_START = $pre_year."1201";
        $ETLDate::LAST_3_MONTH_END = $pre_year."1231";
    }else{
        my $tmp_month = getPreMonth($last_2_month);
        $ETLDate::LAST_3_MONTH_START = $year.$tmp_month."01";
        $ETLDate::LAST_3_MONTH_END = $year.$tmp_month.getMaxDay($year,$tmp_month);
    }
    #��ǰ���ڼ������� + 1
    my $last_3_max_day = getMaxDay(substr($ETLDate::LAST_3_MONTH_START,0,4),substr($ETLDate::LAST_3_MONTH_START,4,2));
    if(($day+1)>$last_3_max_day){
        $ETLDate::LAST_3_MONTH_NEXT_DAY = $ETLDate::LAST_2_MONTH_START;
    }else{
        $ETLDate::LAST_3_MONTH_NEXT_DAY = dateCalc(substr($ETLDate::LAST_3_MONTH_START,0,6)."$day",1);
    }


    #��Ѯ������Ѯĩ
    if($day<=10){
        $ETLDate::CUR_TENDAYS_START = $year.$month."01";
        $ETLDate::CUR_TENDAYS_END = $year.$month."10";
    }elsif($day<=20){
        $ETLDate::CUR_TENDAYS_START = $year.$month."11";
        $ETLDate::CUR_TENDAYS_END = $year.$month."20";
    }else{
        $ETLDate::CUR_TENDAYS_START = $year.$month."21";
        $ETLDate::CUR_TENDAYS_END = $year.$month.getMaxDay($year,$month);
    }
    #����
    $ETLDate::LAST_DAY = dateCalc($tx_date,-1);
    #��ǰ���ڼ�ʮ�� + 1
    $ETLDate::TEND_FIRST_DAY = dateCalc($tx_date,-9);
    if($cast_date_flag && $cast_date_flag eq '1'){
        initCastDate();
    }
}

sub getPreYear {
    my $year = shift;
    my $preYear = $year - 1;
    if($preYear>=1000){
        return $preYear;
    }else{
        return sprintf("%04d",$preYear);
    }
}

sub getPreMonth {
    my $month = shift;
    $month = $month + 0;
    if ( $month == 1 ) {
        return '12';
    }
    elsif ( $month <= 10 ) {
        return '0' . ( $month - 1 );
    }
    else {
        return $month - 1;
    }
}

sub getMaxDay{
    my $yyyy = shift;
    my $mm = shift;
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
#CAST('$tx_date' AS DATE FORMAT 'YYYYMMDD')
sub initCastDate {
    $ETLDate::LAST_MONTH_NEXT_DAY   = castDate($ETLDate::LAST_MONTH_NEXT_DAY  );
    $ETLDate::LAST_3_MONTH_NEXT_DAY = castDate($ETLDate::LAST_3_MONTH_NEXT_DAY);
    $ETLDate::TEND_FIRST_DAY        = castDate($ETLDate::TEND_FIRST_DAY       );

    $ETLDate::MONTH_START           = castDate($ETLDate::MONTH_START          );
    $ETLDate::MONTH_END             = castDate($ETLDate::MONTH_END            );
    $ETLDate::LAST_MONTH_START      = castDate($ETLDate::LAST_MONTH_START     );
    $ETLDate::LAST_MONTH_END        = castDate($ETLDate::LAST_MONTH_END       );
    $ETLDate::LAST_2_MONTH_START    = castDate($ETLDate::LAST_2_MONTH_START );
    $ETLDate::LAST_2_MONTH_END      = castDate($ETLDate::LAST_2_MONTH_END   );
    $ETLDate::YEAR_START            = castDate($ETLDate::YEAR_START           );
    $ETLDate::YEAR_END              = castDate($ETLDate::YEAR_END             );
    $ETLDate::LAST_YEAR_END         = castDate($ETLDate::LAST_YEAR_END        );
    $ETLDate::LAST_YEAR_SAME_DATE   = castDate($ETLDate::LAST_YEAR_SAME_DATE  );
    $ETLDate::LAST_YEAR_NEXT_DATE   = castDate($ETLDate::LAST_YEAR_NEXT_DATE  );
    $ETLDate::LAST_QUAR_START       = castDate($ETLDate::LAST_QUAR_START      );
    $ETLDate::LAST_QUAR_END         = castDate($ETLDate::LAST_QUAR_END        );
    $ETLDate::SEASON_START          = castDate($ETLDate::SEASON_START         );
    $ETLDate::SEASON_END            = castDate($ETLDate::SEASON_END           );
    $ETLDate::CUR_TENDAYS_START     = castDate($ETLDate::CUR_TENDAYS_START    );
    $ETLDate::CUR_TENDAYS_END       = castDate($ETLDate::CUR_TENDAYS_END      );

    $ETLDate::NULL_DATE             = castDate($ETLDate::NULL_DATE            );
    $ETLDate::MIN_DATE              = castDate($ETLDate::MIN_DATE             );
    $ETLDate::MAX_DATE              = castDate($ETLDate::MAX_DATE             );

    $ETLDate::TODAY                 = castDate($ETLDate::TODAY                );
    $ETLDate::LAST_DAY              = castDate($ETLDate::LAST_DAY             );
}

sub castDate{
    my $date = shift;
    if($date){
        return "CAST('$date' AS DATE FORMAT 'YYYYMMDD')";
    }else{
        print "error\n";
    }
}

1;
