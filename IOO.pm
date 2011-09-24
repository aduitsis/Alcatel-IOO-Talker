package IOO;

package IOO::Handle;

use strict;
use warnings;
use Carp;
use Data::Dumper;
use IO::Poll;
use IO::Socket::INET;
use Log::Log4perl qw(:easy);
use constant { 
	READ_SIZE => 2000, #in bytes
	DEFAULT_READ_WAIT_TIME => 10,#in seconds
	
};


Log::Log4perl->easy_init({
        level=>$DEBUG,
        layout => "%M:%L %m%n",
});

my $logger = get_logger();


sub new {
	defined (my $class = shift(@_)) or croak "incorrect call";
	my %option = (@_);
	for my $option ('PeerAddr','PeerPort') {
		defined( $option{$option} ) or croak "Missing $option";
	}
	my $timeout = (defined ( $option{ReadTimeout} ))? $option{ReadTimeout} : DEFAULT_READ_WAIT_TIME;
	defined( my $poll = IO::Poll->new ) or croak "Cannot create poll object";
	defined (my $handle = IO::Socket::INET->new(PeerAddr => $option{PeerAddr},PeerPort =>$option{PeerPort},Proto => 'tcp') ) or croak "Cannot connect";
	$poll->mask($handle => POLLIN);
	DEBUG "handle to $option{PeerAddr}:$option{PeerPort} opened";
	return bless { buffer => IOO::ReadBuffer->new, poll => $poll , handle => $handle, timeout => $timeout },$class;
}

sub syswrite {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	DEBUG "syswrite: $_[0]";
	$self->handle->syswrite($_[0]) || confess $!;
}

sub sysread {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	my $temp;
	$self->handle->sysread($temp,READ_SIZE) || confess $!;
	DEBUG "sysread: $temp";
	return $temp;
}

sub wait_and_read {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	my $n_events = $self->poll->poll($self->timeout) or return;
	confess "poll() returned error" if ($n_events < 0);
	return $self->sysread;
}

sub send_message {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	defined( my $message = shift(@_) ) or confess "incorrect call";	
	confess "Incorrect type of object passed" unless $message->isa("IOO::Message");
	$self->syswrite($message->to_string);
}

sub receive_message {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	#lets first try to extract from the buffer without reading from the handle
	my $msg = $self->buffer->extract;
	return $msg if defined($msg);
	while( defined( my $stuff = $self->wait_and_read() ) ) {
		DEBUG "read succeeded..adding new stuff to buffer";
		$self->buffer->add($stuff); 
		my $msg = $self->buffer->extract;
		return $msg if defined($msg);
	}
	DEBUG "READ_WAIT_TIME timeout elapsed...I/O handle has nothing more to give";
	return;
}

sub receive_many {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	my $sub_ref = shift(@_);
	if ( defined($sub_ref) ) {
		confess "receive_many expects a subroutine reference " if ( defined($sub_ref) && ( ref($sub_ref) ne 'CODE' ) );
		DEBUG "A subroutine reference passed to receive_many.";
	}
	my @bag;
	while ( defined ( my $msg = $self->receive_message ) ) {
		if (defined($sub_ref) && $sub_ref->($msg)) {
			DEBUG "Subroutine said we should stop";
			last;
		}
		push @bag,($msg);
	}
	return @bag;
}	

sub send_and_receive {
	$_[0]->send_message($_[1]);
	return $_[0]->receive_message;
}	

sub send_and_receive_many {
	$_[0]->send_message($_[1]);
	return $_[0]->receive_many($_[2]);
}

sub DESTROY {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	DEBUG "closing handle";	
	$self->handle->close;
}

for my $item (qw(handle poll buffer timeout)) {
        no strict 'refs';#only temporarily 
        *{$item} = sub { return $_[0]->{$item} };
        use strict;
}

package IOO::Message;

use Carp;
use warnings;
use strict;
use Data::Dumper;

sub new {
	defined (my $class = shift(@_)) or confess "incorrect call";
	my %option = (@_);
	defined( $option{command} ) or confess "Missing command-operand";
	return bless { command => $option{command} , payload => $option{payload} },$class;
}

sub str_length {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	return CORE::length($self->command)+CORE::length($self->payload)+2;
}

sub to_string {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	defined( $self->payload ) && return $self->command.'['.$self->payload.']';
	return $self->command.'[]';
}

sub get_parsed_payload {
	return IOO::Payload->new_from_string($_[0]->payload);
}

for my $item (qw(command payload)) {
        no strict 'refs';#only temporarily 
        *{$item} = sub { return $_[0]->{$item} };
        ##*{set_$item} = sub { $_[0]->{$item} = $_[1] };
        use strict;
}

package IOO::Message::Alarm;

use Carp;
use warnings;
use strict;
use Data::Dumper;
use Time::Local;

use base qw(IOO::Message);

sub new_from_plain_message {
	defined (my $class = shift(@_)) or confess "incorrect call";
	defined (my $alarm = shift(@_)) or confess "incorrect call";
	$alarm->command eq 'ALARM_NOTIF' || confess "This is a(n) $alarm->command but it should be an ALARM_NOTIF";
	bless $alarm,$class;
	$alarm->{alarm_record} = $alarm->get_properties;
	return $alarm;
}

sub get_properties {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	my $parsed_payload = $self->get_parsed_payload;
	my %record;
	$record{event_type} = $parsed_payload->[0]->{eventType};
	($record{element},$record{slot},$record{port}) = split('\/',$parsed_payload->[0]->{friendlyName});
	$record{ack_status} = $parsed_payload->[0]->{acknowledgementStatus};
	$record{id} = $parsed_payload->[0]->{currentAlarmId};
	$record{severity} = $parsed_payload->[0]->{perceivedSeverity};
	$record{cause} = $parsed_payload->[0]->{probableCause};
	$record{notification_type} = $parsed_payload->[0]->{notificationType};
	$record{additional_text} = $parsed_payload->[0]->{additionalText};
	$record{event_time} = IOO::Utils::parse_time($parsed_payload->[0]->{eventTime});
	return \%record;
}

sub get_alarm_record {
	return $_[0]->{alarm_record};
}

		
		

package IOO::Session;

use Carp;
use warnings;
use strict;
use Data::Dumper;
use Time::Local;

use base qw(IOO::Handle);
	
sub new {
	defined (my $class = shift(@_)) or confess "incorrect call";
	my %option = (@_);
	for my $option ('Password') {
		defined( $option{$option} ) or confess "Missing $option";
	}
	my $self = bless $class->SUPER::new(@_),$class;
	$self->{password} = $option{Password};
	croak "Cannot open session with peer -- wrong password?" 
		unless ($self->send_and_receive(IOO::Message->new(command=>'CON_REQ',payload=>$self->{password}))->command eq 'CON_CONF');
	$logger->debug("Connected successfully");
	return $self;
}

sub request_data {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	defined( my $command = shift(@_) ) or confess "missing command";	
	return $self->send_and_receive_many(IOO::Message->new(command=>$command),sub {$_[0]->command eq 'DATA_END_NOTIF'});
}

sub is_alive {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	return 1 if ($self->send_and_receive(IOO::Message->new(command=>'HEARTBEAT_REQ'))->command eq 'HEARTBEAT_CONF');
	return;
}

sub get_time {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	my $time_response = $self->send_and_receive(IOO::Message->new(command=>'OS_TIME_REQ'));
	confess "Invalid response: ".$time_response->to_string unless ($time_response->command eq 'OS_TIME_RESP');
	my $payload = $time_response->get_parsed_payload;
	defined( $payload->[0]->{OSTime} ) || confess "Payload cannot be interpreted. Payload str is ".$time_response->payload;
	my ($year,$month,$day,$hour,$minute,$second) = ($payload->[0]->{OSTime} =~ /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/);
	$month--;
	return timelocal($second,$minute,$hour,$day,$month,$year);
}
	


sub end {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	$logger->debug("Ending session");
	$self->send_message(IOO::Message->new(command=>'DISCON_NOTIF'));
}		

package IOO::Session::Alarm;

use Carp;
use warnings;
use strict;
use Data::Dumper;

use base qw(IOO::Session);

sub new {
	defined( my $class = shift(@_) ) or confess "incorrect call";
	my $self = $class->SUPER::new(@_);
	my %option = (@_);
	$self->{as_identity} = (defined($option{ASIdentity}))? $option{ASIdentity} : 1; #let's not make it difficult..default to 1..
	return bless $self,$class;	
}

sub as_identity {
	return $_[0]->{as_identity};	
}

sub get_alarms {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	my @alarms = $self->request_data('LIST_CUR_ALARMS_REQ');
	(shift(@alarms)->command eq 'LIST_CUR_ALARMS_CONF') || confess "Did not get a LIST_CUR_ALARMS_CONF.";
	return map { IOO::Message::Alarm->new_from_plain_message($_) } (@alarms);
}

sub acknowledge_alarm {
	#note-note-note
	#to acknowledge an alarm, we need to send a message like this:
	#ALARMS_ACK_REQ[ackAlarmIds={365297}|asIdentityFilter={1}] 
	#the AS_identity_filter will have to be supplied externally, there is no
	#way that we can learn it from the IOO

	defined( my $self = shift(@_) ) or confess "incorrect call";

	my @alarms = (@_) or confess "Empty alarm list supplied";
	my $alarm_list = join('|',@alarms);
	$logger->debug("Alarms to be purged: ".join(',',@alarms));		

	#for the time being we construct the outgoing payload by hand. Maybe in the future we can encapsulate
	my $payload = "ackAlarmIds={$alarm_list}|asIdentityFilter={".$self->as_identity."}";

	my $message = IOO::Message->new(command => 'ALARMS_ACK_REQ', payload => $payload );
	$logger->debug("We are going to send the following message:".$message->to_string);
	my $response = $self->send_and_receive($message);

	confess "Did not get a ALARMS_ACK_CONF response!" unless ($response->command eq 'ALARMS_ACK_CONF');
}	
		
	
			
		

package IOO::ReadBuffer;

use Carp;
use warnings;
use strict;
use Data::Dumper;

sub new {
	return bless \(my $tmp),$_[0];
}

sub content {
	return $$_[0];
}

sub set {
	$$_[0] = $_[1];
	$logger->debug("setting buffer to ".Dumper($_[0]));
}

sub is_empty {
	return defined($_[0]->content);
}

sub empty {
	$_[0]->set(undef);
}

sub content_length {
	return length $_[0]->content;
}

sub shift_right {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	defined( my $right = shift(@_) ) or confess "incorrect call";
	$logger->debug("shifting ".$self->content." right by $right");
	if($self->content_length == $right) {
		$self->empty;
		return;
	}
	$self->set(substr $self->content,$right);
}

	

sub add {
	$$_[0] .= $_[1];
	$logger->debug("Buffer is now $$_[0]");
}

sub extract {
	defined( my $self = shift(@_) ) or confess "incorrect call";
	unless (defined($self->content)) {
		$logger->debug("empty content...nothing to do");
		return;
	}
	my ($command,$payload) = ( $self->content =~ /^([A-Z_]+)\[(.*?)\]/ );
	unless(defined($command)) {
		$logger->debug("no command found in buffer");
		return;
	}
	my $message = IOO::Message->new(command=>$command,payload=>$payload);
	$logger->debug("message is ".$message->to_string);
	$self->shift_right($message->str_length);
	return $message;
}




package IOO::Payload;

use strict;
use warnings;
use Carp;
use Data::Dumper;

my $DEBUG = 0;


sub new_from_string {
	defined (my $class = shift(@_)) or confess "Incorrect call";
	defined (my $str = shift(@_)) or confess "Incorrect call";
	
	return bless parse_payload($str),$class;
}


sub parse_payload {
	defined (my $payload_str = shift(@_)) or confess "Incorrect call to parse";
	my @items = ( $payload_str =~ /([=\|\{\}]|[^=\|\{\}]+)/g );
	#print Dumper(@items);
	return parse_items(1,\@items);
}


sub parse_items {
	defined (my $level = shift(@_)) or confess "Incorrect call to parse";
	defined (my $array_ref = shift(@_)) or confess "Incorrect call to parse";
	my ($data,$key);
	my @values;
	my $flag = 0;
	while(defined(my $str = shift @{$array_ref})) {
		my $last_hash = ((!@values)&&(ref($data->[$#{$data}]) eq 'HASH'))? $data->[$#{$data}] : undef;
		$DEBUG&&print STDERR "($level) -------------------------------------------------------------\n";
		$DEBUG&&print STDERR "($level) data = ".Dumper($data);
		$DEBUG&&print STDERR "($level) last_hash = ".Dumper($last_hash);
		$DEBUG&&print STDERR "($level) values = ".Dumper(\@values);

		$DEBUG&&print STDERR "($level) pop: $str \n";
		if ($str =~ /[^=\|\{\}]+/) {
			if (defined($key)) {
				$DEBUG&&print STDERR "($level) we have a key $key \n";
				if (defined($last_hash)) { 
					$DEBUG&&print STDERR "($level) attaching to last_hash \n";
					$last_hash->{$key} = $str ;
				}
				else {
					$DEBUG&&print STDERR "($level) pushing to values \n";
					push @values, { $key => $str }; 
				}
				$key = undef;
			} 
			else {
				push @values,($str);
			}
			#print STDERR "Values is ".Dumper(\@values),"\n";
		}
		elsif ($str eq '{') {
			my $tmp = parse_items(($level+1),$array_ref);
			if (defined($key)) {
				(defined($last_hash))? $last_hash->{$key} = $tmp : push @values, { $key => $tmp }; 
				$key = undef;
			} 
			else {
				push @values,($tmp);
			}
		}
		elsif ($str eq '=') {
			$key = pop(@values);
			$DEBUG&&print STDERR "($level) key is $key \n";
			confess "($level) undefined key from last element of values" if (!defined($key));
		}		
		elsif ( ($str eq '|') || ($str eq '}') ) {
			#print STDERR "($level) end of run detected \n";
			$flag = 1;
		} 
		else {
			confess "($level) Abnormal item '$str' ...  array_ref is ".Dumper($array_ref);
		}

		
		$flag = 1 unless(@{$array_ref}); #if we'd put that statement in the if/then/else, we would miss it 
		if ( $flag ) {

			#special case: some MORON preferred to say 'SES=' instead of 'SES=0' etc.
			if (defined($key)) {
				my $default_value = 0;
				(defined($last_hash))? $last_hash->{$key} = $default_value : push @values, { $key => $default_value }; 
				$key = undef;
			} 

			#clear the contents
			###print STDERR "($level) ".Dumper($data);
			push @{$data},(@values);
			$key = undef;
			@values = ();
			$flag = 0;
			#if '}' we need to exit the loop ... no more work here for us
		}

		last if ($str eq '}'); #if } is encountered, then end the loop and wrap up

		$DEBUG&&print STDERR "($level) data = ".Dumper($data);
		$DEBUG&&print STDERR "($level) last_hash = ".Dumper($last_hash);
		$DEBUG&&print STDERR "($level) values = ".Dumper(\@values);
		$DEBUG&&print STDERR "($level) -------------------------------------------------------------\n";
	}#while loop
	$DEBUG&&print STDERR "($level) returning ".Dumper($data)."\n";
	$DEBUG&&print STDERR "key is $key\n" if (defined($key));
	confess "Value is not empty as it should!!".Dumper(\@values) if (@values);
	return $data;
	
}

package IOO::Utils;

use strict;
use warnings;
use Carp;
use Data::Dumper;
use Time::Local;

sub parse_time {
	my ($year,$month,$day,$hour,$minute,$second) = ($_[0] =~ /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/);
	$month--;
	return timelocal($second,$minute,$hour,$day,$month,$year);
}


1;
