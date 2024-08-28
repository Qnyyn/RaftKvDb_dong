#include"raftRpcUtil.h"

#include<mprpcchannel.h>
#include<mprpccontroller.h>

bool RaftRpcUtil::AppendEntries(raftRpcProtoc::AppendEntriesArgs *args,raftRpcProtoc::AppendEntriesReply *response){
    MprpcController controller;
    stub_->AppendEntries(&controller,args,response,nullptr);
    return !controller.Failed();
}
bool RaftRpcUtil::InstallSnapshot(raftRpcProtoc::InstallSnapshotRequest *args,raftRpcProtoc::InstallSnapshotResponse *response){
    MprpcController controller;
    stub_->InstallSnapshot(&controller,args,response,nullptr);
    return !controller.Failed();
}
bool RaftRpcUtil::RequestVote(raftRpcProtoc::RequestVoteArgs *args,raftRpcProtoc::RequestVoteReply *response){
    MprpcController controller;
    stub_->RequestVote(&controller,args,response,nullptr);
    return !controller.Failed();
}

//先开启服务器，再尝试连接其他节点，中间给个间隔时间，等待其他的rpc服务器节点启动
RaftRpcUtil::RaftRpcUtil(std::string ip,short port){
    //发送rpc设置
    //stub的初始化需要一个channel去真正调用
    stub_ = new raftRpcProtoc::raftRpc_Stub(new MprpcChannel(ip,port,true));
}
RaftRpcUtil::~RaftRpcUtil(){
    delete stub_;
}