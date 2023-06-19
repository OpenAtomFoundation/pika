start_server {} {
    set i [r info]
    regexp {pika_version:(.*?)\r\n} $i - version
    regexp {pika_git_sha:(.*?)\r\n} $i - sha1
    puts "Testing Pika version $version ($sha1)"
}
