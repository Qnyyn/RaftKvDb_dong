#pragma once
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "ApplyMsg.h"
#include "Persister.h"
#include "boost/any.hpp"
#include "boost/serialization/serialization.hpp"
#include "config.h"
#include "raftRpcUtil.h"
#include "util.h"

enum NetState{
    Disconnected,
    AppNormal,
};

enum VoteState{
    Killed,
    Voted,//投过票了
    Expire,//投票（消息、竞选者）过期
    Normal
};

class Raft:public raftRpcProtoc::raftRpc{
public:
    //* 方法
    //本地appendentries方法，日志同步+心跳 rpc
    void AppendEntries1(const raftRpcProtoc::AppendEntriesArgs *args,raftRpcProtoc::AppendEntriesReply *reply);
    //raft向状态机定时写入
    void applierTicker();
    //发起选举
    void doElection();
    //发起心跳，只有leader才需要发起心跳
    void doHeartBeat();
    //选举超时定时器
    void electionTimeOutTicker();
    //心跳维护定时器，检查是否需要发起心跳
    void leaderHearBeatTicker();
    void leaderSendSnapShot(int server);
    //持久化
    void persist();
    //接收远端发来的请求投票函数
    void RequestVote(const raftRpcProtoc::RequestVoteArgs *args,raftRpcProtoc::RequestVoteReply *reply);
    //请求其他节点的投票
    bool sendRequestVote(int server,std::shared_ptr<raftRpcProtoc::RequestVoteArgs> args,std::shared_ptr<raftRpcProtoc::RequestVoteReply> reply,std::shared_ptr<int> votedNum);
    //Leader发送心跳后，对心跳的回复进行对应的处理
    bool sendAppendEntries(int server,std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply,std::shared_ptr<int> appendNums);
    //安装快照本地方法
    void InstallSnapshot(const raftRpcProtoc::InstallSnapshotRequest *args,raftRpcProtoc::InstallSnapshotResponse *reply);
    //给上层的kvserver层发送消息
    void pushMsgToKvServer(ApplyMsg msg);
    void readPersist(std::string data);
    //发布来一个新日志
    //即kv-server主动发起，请求raft保存snapshot里面的数据，index是用来表示snapshot快照执行到了哪条命令
    void Start(Op command,int *newLogIndex,int *newLogTerm,bool *isLeader);
    // 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
    // 即服务层主动发起请求raft保存snapshot里面的数据，index是用来表示snapshot快照执行到了哪条命令
    void Snapshot(int index,std::string snapshot);
    //快照相关，非重点
    bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot);
    std::string persistData();
    //初始化函数
    void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers,int me,std::shared_ptr<Persister> persister,std::shared_ptr<LockQueue<ApplyMsg>> applyCh);
public:
    //* 重写框架的RPC服务方法，远端的方法会先走到这里，然后在内部调用本地函数，在发回
    void AppendEntries(google::protobuf::RpcController *controller,const ::raftRpcProtoc::AppendEntriesArgs *request,
                        ::raftRpcProtoc::AppendEntriesReply *response,::google::protobuf::Closure *done) override;
    void InstallSnapshot(google::protobuf::RpcController *controller,const ::raftRpcProtoc::InstallSnapshotRequest *request,
                        ::raftRpcProtoc::InstallSnapshotResponse *response,::google::protobuf::Closure *done) override;
    void RequestVote(google::protobuf::RpcController *controller,const ::raftRpcProtoc::RequestVoteArgs *request,
                        ::raftRpcProtoc::RequestVoteReply *response,::google::protobuf::Closure *done) override;

    //* ===================================================================================================================
    //* 辅助函数(非重点)
    std::vector<ApplyMsg> getApplyLogs();
    int getNewCommandIndex();
    void getPrevLogInfo(int server,int *preIndex,int *preTerm);
    //查看当前节点是否是leader
    void GetState(int *term,bool *isLeader);
    //leader更新commitIndex
    void leaderUpdateCommitIndex();
    //对应的Index的日志是否匹配,只需要Index和Term就可以知道是否匹配
    bool matchLog(int logIndex,int logTerm);
    //检查当前节点是否含有最新的日志
    bool UpToDate(int index,int term);
    int getLastLogIndex();
    int getLastLogTerm();
    void getLastLogIndexAndTerm(int *lastLogIndex,int *lastLogTerm);
    int getLogTermFromLogIndex(int logIndex);
    int GetRaftStateSize();
    //设计快照之后logIndex不能与在日志中的数组下标相等了，根据logIndex找到其在日志数组中的位置
    int getSliceIndexFromLogIndex(int logIndex);


private:
    std::condition_variable m_cv;
    std::mutex m_mtx;
    //* 外部内容

    //与其他rpc节点通信的stub，也是channel
    std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;

    //持久层
    std::shared_ptr<Persister> m_persister;

    //* 节点本身内容
    int m_me;

    int m_currentTerm;

    int m_votedFor;
    //日志条目数组
    std::vector<raftRpcProtoc::LogEntry> m_logs;
    //达成共识的提交索引
    //当follower收到leader发来的commitIndex，然后把lastApplied到commit的之间日志条目都应用到状态机
    int m_commitIndex;
    //已经应用到状态机上的索引
    int m_lastApplied;

    //由leader维护的两个条目
    std::vector<int> m_nextIndex;
    std::vector<int> m_matchIndex;
    enum Status{Follower,Candidate,Leader};

    //身份
    Status m_status;
    //raft 与 kvserver沟通
    std::shared_ptr<LockQueue<ApplyMsg>> applyChan;

    //选举超时时间
    std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;

    //心跳超时,leader用
    std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime;

    //存储了快照中最后的一个日志的index和term
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;
private:
    //负责序列化raft节点状态
    class BoostPersistRaftNode {
        public:
            friend class boost::serialization::access;
            // When the class Archive corresponds to an output archive, the
            // & operator is defined similar to <<.  Likewise, when the class Archive
            // is a type of input archive the & operator is defined similar to >>.
            template <class Archive>
            void serialize(Archive &ar, const unsigned int version) {
                ar &m_currentTerm;
                ar &m_votedFor;
                ar &m_lastSnapshotIncludeIndex;
                ar &m_lastSnapshotIncludeTerm;
                ar &m_logs;
            }
            int m_currentTerm;
            int m_votedFor;
            int m_lastSnapshotIncludeIndex;
            int m_lastSnapshotIncludeTerm;
            std::vector<std::string> m_logs;
            std::unordered_map<std::string, int> umap;

        public:
    };
};