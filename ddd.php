<?php

for ($i = 1; $i < 10; $i++) {
    $i++;
    file_put_contents("request.log", '{"user_agent":"user_agent","session_id":"khr1pdgmp3b0qgripd91uvuluv","add_time":'.+time().',"create_time":"2022-06-24 15:49:23","ip":"222.181.205.203","site":"mkt.444.com","url":"er_id=198444","http_version":"0","id":'.$i.',"address":"重庆重庆市"}'.PHP_EOL, FILE_APPEND|LOCK_EX);
}
