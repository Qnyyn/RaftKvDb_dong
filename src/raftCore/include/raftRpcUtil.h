#pragma once
#include"raftRpc.pb.h"

//维护当前节点对其他一个节点的所有RPC发送通信的功能
class RaftRpcUtil{
public:
    //调用需要通过stub桩
    raftRpcProtoc::raftRpc_Stub *stub_;
    bool AppendEntries(raftRpcProtoc::AppendEntriesArgs *args,raftRpcProtoc::AppendEntriesReply *response);
    bool InstallSnapshot(raftRpcProtoc::InstallSnapshotRequest *args,raftRpcProtoc::InstallSnapshotResponse *response);
    bool RequestVote(raftRpcProtoc::RequestVoteArgs *args,raftRpcProtoc::RequestVoteReply *response);
public:

    //远端的ip和端口
    RaftRpcUtil(std::string ip,short port);
    ~RaftRpcUtil();
};