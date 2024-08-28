#pragma once

#include <boost/any.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include "kvServerRpc.pb.h"
#include "raft.h"
#include "skipList.h"

class kvServer:raftKVRpcProctoc::kvServerRpc{
public:
    kvServer() = delete;
    kvServer(int me,int maxraftstate,std::string nodeInfoFileName,short port);
    
    //构造启动了
    //void StartKvServer();

    void DprintfKvDb();

    void ExecuteAppendOpOnKvDb(Op op);

    void ExecuteGetOpOnKvDb(Op op,std::string *value,bool *exist);

    void ExecutePutOpOnKvDb(Op op);

    
    //* 从raft节点中获取消息
    void GetCommandFromRaft(ApplyMsg message);

    bool ifRequestDuplicate(std::string clientId,int requestId);

    //* clerk 使用Rpc远程调用到本地
    void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args,raftKVRpcProctoc::PutAppendReply *reply);
    void Get(const raftKVRpcProctoc::GetArgs *args,raftKVRpcProctoc::GetReply *reply);
    //一直等待raft传来的applyCh
    void ReadRaftApplyCommandLoop();

    void ReadSnapShotToInstall(std::string snapshot);
    //把确认并提交的操作放入这个waitchan供客户端确认，一种异步机制
    bool SendMessageToWaitChan(const Op &op,int raftIndex);

    //检查是否需要制作快照，需要的话就向raft之下制作快照
    void IfNeedToSendSnapShotCommand(int raftIndex,int proportion);

    void GetSnapShotFromRaft(ApplyMsg message);

    std::string MakeSnapShot();
public:
    //* Rpc方法
    void PutAppend(google::protobuf::RpcController *controller,const ::raftKVRpcProctoc::PutAppendArgs *request,
                    ::raftKVRpcProctoc::PutAppendReply *response,::google::protobuf::Closure *done) override;
    void Get(google::protobuf::RpcController *controller,const ::raftKVRpcProctoc::GetArgs *request,
                    ::raftKVRpcProctoc::GetReply *response,::google::protobuf::Closure *done) override;
private:
    std::mutex m_mtx;
    int m_me;
    std::shared_ptr<Raft> m_raftNode;
    std::shared_ptr<LockQueue<ApplyMsg>> applyChan; //kvServer 和 raft节点的通信channel
    int m_maxRaftState;//snapshot if log grows this big

    std::string m_serializedKvData;//序列化后的kv数据
    SkipList<std::string,std::string> m_skipList{18};//跳表存储引擎
    //这个其实是返回给客户端的结果
    std::unordered_map<int,LockQueue<Op> *> waitApplyCh;  //value 是Op类型的管道
    //跟踪用户最后一次请求的ID，这样可以忽略或拒绝重复的请求
    std::unordered_map<std::string,int> m_lastRequestId;//clientId->requestId  //一个kv服务器可能连接多个client

    //假如快照点最后索引为5，那么这个变量就是6，当raft节点重启后，会把6之前的日志应用到状态机
    //这样追赶就只需要从6这个节点开始往后追赶就行了
    int m_lastSnapShotRaftLogIndex;

private:
    //* 序列化方法
    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive &ar,const unsigned int version){
        ar &m_serializedKvData;
        ar &m_lastRequestId;
    }

    std::string getSnapShotData(){
        m_serializedKvData = m_skipList.dump_file();
        std::stringstream ss;
        boost::archive::text_oarchive oa(ss);
        oa << *this;
        m_serializedKvData.clear();
        return ss.str();
    }

    void parseFromString(const std::string &str){
        std::stringstream ss(str);
        boost::archive::text_iarchive ia(ss);
        ia >> *this;
        m_skipList.load_file(m_serializedKvData);
        m_serializedKvData.clear();
    }
};