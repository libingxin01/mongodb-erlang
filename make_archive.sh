#! /bin/bash
mkdir mongodb-erlang
mkdir bson
cp -r ebin mongodb-erlang
cp -r doc mongodb-erlang
cp -r src mongodb-erlang
cp -r include mongodb-erlang
cp -r deps/bson/ebin bson
cp -r deps/bson/doc bson
cp -r deps/bson/src bson
cp -r deps/bson/include bson
rm mongodb-erlang.zip
zip -q mongodb-erlang.zip -r mongodb-erlang -r bson
rm -r mongodb-erlang
rm -r bson


