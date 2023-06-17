start_server {tags {"maxmemory"}} {
    test "Without maxmemory small integers are shared" {
        set maxm [r config get maxmemory]
        assert {$maxm > 1}
    }

}
