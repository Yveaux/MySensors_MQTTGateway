#!/usr/bin/perl -w

#
# MQTT Gateway - A gateway to exchange MySensors messages from an
#                ethernet gateway with an MQTT broker.
#
# Created by Ivo Pullens, Emmission, 2014 -- www.emmission.nl
#
# Tested with MySensors 1.4.1 (July 17, 2015) (www.mysensors.org)
#           & MySensors 1.5   (Aug  17, 2015)
# and Mosquitto MQTT Broker (mosquitto.org) on Ubuntu Linux 12.04.
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

local $ENV{ANYEVENT_MQTT_DEBUG} = 1;

use strict;
use Cwd;
use IO::Socket::INET;
use Net::MQTT::Constants;
use AnyEvent::Socket;
use AnyEvent::MQTT;
use AnyEvent::Strict;   # check parameters passed to callbacks 
use Data::Dumper;
use List::Util qw(first);
use Storable;
use Time::Zone;
use v5.14;              # Requires support for given/when constructs
use Device::SerialPort;

#-- Message types
use enum qw { C_PRESENTATION=0 C_SET C_REQ C_INTERNAL C_STREAM };
use constant commandToStr => qw( C_PRESENTATION C_SET C_REQ C_INTERNAL C_STREAM );

#-- Variable types
use enum qw { V_TEMP=0 V_HUM V_LIGHT V_DIMMER V_PRESSURE V_FORECAST V_RAIN
        V_RAINRATE V_WIND V_GUST V_DIRECTION V_UV V_WEIGHT V_DISTANCE
        V_IMPEDANCE V_ARMED V_TRIPPED V_WATT V_KWH V_SCENE_ON V_SCENE_OFF
        V_HEATER V_HEATER_SW V_LIGHT_LEVEL V_VAR1 V_VAR2 V_VAR3 V_VAR4 V_VAR5
        V_UP V_DOWN V_STOP V_IR_SEND V_IR_RECEIVE V_FLOW V_VOLUME V_LOCK_STATUS
        V_DUST_LEVEL V_VOLTAGE V_CURRENT };
use constant variableTypesToStr => qw{ V_TEMP V_HUM V_LIGHT V_DIMMER V_PRESSURE V_FORECAST V_RAIN
        V_RAINRATE V_WIND V_GUST V_DIRECTION V_UV V_WEIGHT V_DISTANCE
        V_IMPEDANCE V_ARMED V_TRIPPED V_WATT V_KWH V_SCENE_ON V_SCENE_OFF
        V_HEATER V_HEATER_SW V_LIGHT_LEVEL V_VAR1 V_VAR2 V_VAR3 V_VAR4 V_VAR5
        V_UP V_DOWN V_STOP V_IR_SEND V_IR_RECEIVE V_FLOW V_VOLUME V_LOCK_STATUS
        V_DUST_LEVEL V_VOLTAGE V_CURRENT };

sub variableTypeToIdx
{
  my $var = shift;
  return first { (variableTypesToStr)[$_] eq $var } 0 .. scalar(variableTypesToStr);
}

#-- Internal messages
use enum qw { I_BATTERY_LEVEL=0 I_TIME I_VERSION I_ID_REQUEST I_ID_RESPONSE
        I_INCLUSION_MODE I_CONFIG I_FIND_PARENT I_FIND_PARENT_RESPONSE
        I_LOG_MESSAGE I_CHILDREN I_SKETCH_NAME I_SKETCH_VERSION
        I_REBOOT I_GATEWAY_READY };
use constant internalMessageTypesToStr => qw{ I_BATTERY_LEVEL I_TIME I_VERSION I_ID_REQUEST I_ID_RESPONSE
        I_INCLUSION_MODE I_CONFIG I_FIND_PARENT I_FIND_PARENT_RESPONSE
        I_LOG_MESSAGE I_CHILDREN I_SKETCH_NAME I_SKETCH_VERSION
        I_REBOOT I_GATEWAY_READY };

#-- Sensor types
use enum qw { S_DOOR=0 S_MOTION S_SMOKE S_LIGHT S_DIMMER S_COVER S_TEMP S_HUM S_BARO S_WIND
        S_RAIN S_UV S_WEIGHT S_POWER S_HEATER S_DISTANCE S_LIGHT_LEVEL S_ARDUINO_NODE
        S_ARDUINO_REPEATER_NODE S_LOCK S_IR S_WATER S_AIR_QUALITY S_CUSTOM S_DUST
        S_SCENE_CONTROLLER };
use constant sensorTypesToStr => qw{ S_DOOR S_MOTION S_SMOKE S_LIGHT S_DIMMER S_COVER S_TEMP S_HUM S_BARO S_WIND
        S_RAIN S_UV S_WEIGHT S_POWER S_HEATER S_DISTANCE S_LIGHT_LEVEL S_ARDUINO_NODE
        S_ARDUINO_REPEATER_NODE S_LOCK S_IR S_WATER S_AIR_QUALITY S_CUSTOM S_DUST
        S_SCENE_CONTROLLER };
        
#-- Datastream types
use enum qw { ST_FIRMWARE_CONFIG_REQUEST=0 ST_FIRMWARE_CONFIG_RESPONSE ST_FIRMWARE_REQUEST ST_FIRMWARE_RESPONSE
        ST_SOUND ST_IMAGE };
use constant datastreamTypesToStr => qw{ ST_FIRMWARE_CONFIG_REQUEST ST_FIRMWARE_CONFIG_RESPONSE ST_FIRMWARE_REQUEST ST_FIRMWARE_RESPONSE
        ST_SOUND ST_IMAGE };

#-- Payload types
use enum qw { P_STRING=0 P_BYTE P_INT16 P_UINT16 P_LONG32 P_ULONG32 P_CUSTOM P_FLOAT32 };
use constant payloadTypesToStr => qw{ P_STRING P_BYTE P_INT16 P_UINT16 P_LONG32 P_ULONG32 P_CUSTOM P_FLOAT32 };

use constant topicRoot => '/mySensors';    # Omit trailing slash

my $mqttHost = 'localhost';         # Hostname of MQTT broker
my $mqttPort = 1883;                # Port of MQTT broker

# -- Switch to choose between Ethernet gateway or serial gateway
my $useSerial = 0;

# -- Settings when using Ethernet gateway
my $mysnsHost   = '192.168.1.10';   # IP of MySensors Ethernet gateway
my $mysnsPort   = 5003;             # Port of MySensors Ethernet gateway

# -- Settings when using Serial gateway
my $serialPort   = "/dev/ttyUSB2";
my $serialBaud   = 115200;
my $serialBits   = 8;
my $serialParity = "none";
my $serialStop   = 1;
  
  
my $retain = 1;
my $qos = MQTT_QOS_AT_LEAST_ONCE;
my $keep_alive_timer = 120;
my $subscriptionStorageFile = "subscriptions.storage";
my $serialDevice;

my @subscriptions;

sub debug
{
  print "##" . join(" ", @_)."\n";
}

sub initialiseSerialPort
{
  print "Initialising serial port  $serialPort\n";
  $serialDevice = tie (*FH, 'Device::SerialPort', $serialPort) || die "Can't tie: $!";
  $serialDevice->baudrate($serialBaud);
  $serialDevice->databits($serialBits);
  $serialDevice->parity($serialParity);
  $serialDevice->stopbits($serialStop);

  my $tEnd = time()+2; # 2 seconds in future
  while (time()< $tEnd) { # end latest after 2 seconds
    my $c = $serialDevice->lookfor(); # char or nothing
    next if $c eq ""; # restart if noting
    print $c; # uncomment if you want to see the gremlin
    last;
  }
  while (1) { # and all the rest of the gremlins as they come in one piece
    my $c = $serialDevice->lookfor(); # get the next one
    last if $c eq ""; # or we're done
    print $c; # uncomment if you want to see the gremlin
  }
}

sub parseMsg
{
  my $txt = shift;
  my @fields = split(/;/,$txt);
  my $msgRef = { radioId => $fields[0],
                 childId => $fields[1],
                 cmd     => $fields[2],
                 ack     => $fields[3],
                 subType => $fields[4],
                 payload => $fields[5] };
  return $msgRef;
}

sub parseTopicMessage
{
  my ($topic, $message) = @_;
  if ($topic !~ s/^@{[topicRoot]}\/?//)
  {
    # Topic root doesn't match
    return undef;
  }
  my @fields = split(/\//,$topic);
  my $msgRef = { radioId => int($fields[0]),
                 childId => int($fields[1]),
                 cmd     => C_SET,
                 ack     => 0,   # No way to tell ack from topic
                 subType => variableTypeToIdx( $fields[2] ),
                 payload => $message };
#  print Dumper($msgRef);
  return $msgRef;
}

sub createMsg
{
  my $msgRef = shift;
  my @fields = ( $msgRef->{'radioId'},
                 $msgRef->{'childId'},
                 $msgRef->{'cmd'},
                 $msgRef->{'ack'},
                 $msgRef->{'subType'},
                 defined($msgRef->{'payload'}) ? $msgRef->{'payload'} : "" );
  return join(';', @fields);
}

sub subTypeToStr
{
  my $cmd = shift;
  my $subType = shift;
  # Convert subtype to string, depending on message type
  given ($cmd)
  {
    $subType = (sensorTypesToStr)[$subType] when C_PRESENTATION;
    $subType = (variableTypesToStr)[$subType] when C_SET;
    $subType = (variableTypesToStr)[$subType] when C_REQ;
    $subType = (internalMessageTypesToStr)[$subType] when C_INTERNAL;
    default { $subType = "<UNKNOWN_$subType>" }
  }  
  return $subType;
}

sub dumpMsg
{
  my $msgRef = shift;
  my $cmd = (commandToStr)[$msgRef->{'cmd'}];
  my $st = subTypeToStr( $msgRef->{'cmd'}, $msgRef->{'subType'} );
  printf("Rx: fr=%03d ci=%03d c=%03d(%-14s) st=%03d(%-16s) ack=%d %s\n", $msgRef->{'radioId'}, $msgRef->{'childId'}, $msgRef->{'cmd'}, $cmd, $msgRef->{'subType'}, $st, $msgRef->{'ack'}, defined($msgRef->{'payload'}) ? "'".$msgRef->{'payload'}."'" : "");
}

sub createTopic
{
  my $msgRef = shift;
  my $st = subTypeToStr( $msgRef->{'cmd'}, $msgRef->{'subType'} );
  # Note: Addendum to spec: a topic ending in a slash is considered equivalent to the same topic without the slash -> So we leave out the trailing slash
  return sprintf("%s/%d/%d/%s", topicRoot, $msgRef->{'radioId'}, $msgRef->{'childId'}, $st);
}

my $mqtt;
# Open a separate socket connection for sending.
# TODO: I guess this should not be required.... Figure out how...
my $socktx = undef;
if(!$useSerial)
{
  $socktx = new IO::Socket::INET (
    PeerHost => $mysnsHost,
    PeerPort => $mysnsPort,
    Proto => 'tcp',
  ) or die "ERROR in Socket Creation : $!\n";
}

# Callback called when MQTT generates an error
sub onMqttError
{
  my ($fatal, $message) = @_;
  if ($fatal) {
    die $message, "\n";
  } else {
    warn $message, "\n";
  }
}

# Callback when MQTT broker publishes a message
sub onMqttPublish
{
  my($topic, $message) = @_;
  debug("$topic:$message");
  my $msgRef = parseTopicMessage($topic, $message);
  my $mySnsMsg = createMsg($msgRef);
  debug($mySnsMsg); 
  print "Publish $topic: $message => $mySnsMsg\n";
  print $socktx "$mySnsMsg\n" if (defined $socktx);
}                                  

# Callback called when socket connection generates an error
sub onSocketError
{
  my ($handle, $fatal, $message) = @_;
  if ($fatal) {
    die $message, "\n";
  } else {
    warn $message, "\n";
  }
}

sub onSocketDisconnect
{
  my ($handle) = @_;
  $handle->destroy();
}

sub subscribe
{
  my $topic = shift;
  if (!($topic ~~ @subscriptions))
  {
    print "Subscribe '$topic'\n";
    $mqtt->subscribe(
        topic    => $topic,
        callback => \&onMqttPublish,
        qos      => $qos );
  #      $cv->recv; # subscribed,

    # Add topic to list of subscribed topics, if not already present.
    push(@subscriptions, $topic);
  }  else  {
    print "Subscribe '$topic' ignored -- already subscribed\n";
  }
}

sub unsubscribe
{
  my $topic = shift;
  if ($topic ~~ @subscriptions)
  {
    print "Unsubscribe '$topic'\n";
    $mqtt->unsubscribe(
        topic    => $topic
      );

    # Remove topic from list of subscribed topics, if present.
    @subscriptions = grep { !/^$topic$/ } @subscriptions;
  }  else  {
    print "Unsubscribe '$topic' ignored -- not subscribed\n";
  }
}

sub gettime
{
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  $year += 1900;
  $mon++;
  return sprintf("%04d%02d%02d-%02d:%02d:%02d", $year, $mon, $mday, $hour, $min, $sec);
}

sub publish
{
  my ($topic, $message) = @_;
  $message = "" if !defined($message);
  print "Publish '$topic':'$message'\n";
  $mqtt->publish(
      topic   => $topic,
      message => $message,
      retain  => 1,
      qos     => $qos,
    );

  # Publish timestamp of last update
  $mqtt->publish(
    topic   => "$topic/lastupdate",
    message => gettime(),
    retain  => 1,
    qos     => $qos,
  );
}

sub onNodePresenation
{
  # Node announces itself on the network
  my $msgRef = shift;
  my $radioId = $msgRef->{'radioId'};

  # Remove existing subscriptions for this node
  foreach my $topic( grep { /^@{[topicRoot]}\/$radioId\// } @subscriptions )
  {
    unsubscribe($topic);
  }
}

sub onRequestTime
{
  # Node requests current time from gateway
  my $msgRef = shift;
  # Get seconds since epoch, but correct for local time zone
  my $ts = time + tz_local_offset();
  $msgRef->{'payload'} = $ts;
  my $mySnsMsg = createMsg($msgRef);
  print "Request time: $ts => $mySnsMsg\n";
  print $socktx "$mySnsMsg\n" if (defined $socktx); 
}

sub handleRead
{
  my $data = shift;
  chomp($data);
  debug($data);

  my $msgRef = parseMsg($data);
  dumpMsg($msgRef);
  given ($msgRef->{'cmd'})
  {
    when ([C_PRESENTATION, C_SET, C_INTERNAL])
    {
      onNodePresenation($msgRef) if (($msgRef->{'childId'} == 255) && ($msgRef->{'cmd'} == C_PRESENTATION));
      onRequestTime($msgRef)     if (($msgRef->{'cmd'} == C_INTERNAL) && ($msgRef->{'subType'} == I_TIME));
      publish( createTopic($msgRef), $msgRef->{'payload'} );
    }
    when (C_REQ)
    {
      subscribe( createTopic($msgRef) );
    }
    default
    {
      # Ignore
    }
  }
}
  
sub onSocketRead
{
  my $handle = shift;
  handleRead($handle->rbuf);
  
  # Clear the buffer
  $handle->rbuf = "";
}

sub onSerialRead
{
  handleRead(shift);
}

sub doShutdown
{
  store(\@subscriptions, $subscriptionStorageFile) ;
}

sub onCtrlC
{
  print "\nShutdown requested...\n";
  doShutdown();
  exit;
}

# Flush STDOUT
$| = 1;

# Install Ctrl-C handler
$SIG{INT} = \&onCtrlC;

initialiseSerialPort()  if ($useSerial);

while (1)
{
  # Run code in eval loop, so die's are caught with error message stored in $@
  # See: https://sites.google.com/site/oleberperlrecipes/recipes/06-error-handling/00---simple-exception
  local $@ = undef;
  eval
  {
    $mqtt = AnyEvent::MQTT->new(
          host => $mqttHost,
          port => $mqttPort,
          keep_alive_timer => $keep_alive_timer,
          on_error => \&onMqttError,
#          clean_session => 0,   # Retain client subscriptions etc
          client_id => 'MySensors',
          );

    my $cv;
    
    if ($useSerial)
    {
      my $handle;
      $handle = AnyEvent::Handle->new(
        fh => $serialDevice->{'HANDLE'},
        on_error => sub {
          my ($handle, $fatal, $message) = @_;
          $handle->destroy;
          undef $handle;
          $cv->send("$fatal: $message");
        },
        on_read => sub {
          my $handle = shift;
          $handle->push_read(line => sub {
            my ($handle, $line) = @_;
            onSerialRead($line);
          });
        }
      );
    }
    else
    {
      tcp_connect($mysnsHost, $mysnsPort, sub {
          my $sock = shift or die "FAIL";
          my $handle;
          $handle = AnyEvent::Handle->new(
               fh => $sock,
               on_error => \&onSocketError,
               on_eof => sub {
                  print "DISCONNECT!\n";
                  $handle->destroy();
               },
               on_connect => sub {
                 my ($handle, $host, $port) = @_;
                  print "Connected to MySensors gateway at $host:$port\n";
               },
               on_read => \&onSocketRead );
        }  );
    }
    
    # On shutdown active subscriptions will be saved to file. Read this file here and restore subscriptions.
    if (-e $subscriptionStorageFile)
    {
      my $subscrRef = retrieve($subscriptionStorageFile);
      print "Restoring previous subscriptions:\n" if @$subscrRef;
      foreach my $topic(@$subscrRef)
      {
        subscribe($topic);
      }
    }
    
    $cv = AnyEvent->condvar;
    $cv->recv; 
  }
  or do
  { 
    print "Exception: ".$@."\n";
    doShutdown();
    print "Restarting...\n";
  }
}
