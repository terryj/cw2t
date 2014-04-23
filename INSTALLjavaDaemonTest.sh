mkdir -p javaDaemonTest
cd javaDaemonTest
wget http://www.source-code.biz/snippets/java/javaDaemonTest.tar.gz
tar -xzf javaDaemonTest.tar.gz
javac JavaDaemonTest.java
./javaDaemonTest.sh install
rcjavaDaemonTest start
rcjavaDaemonTest status
... wait a bit to let the test program write some output lines into the log file ...
rcjavaDaemonTest stop
rcjavaDaemonTest status
less javaDaemonTest.log
