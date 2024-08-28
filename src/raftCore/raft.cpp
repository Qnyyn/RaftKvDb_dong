#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include "config.h"
#include "util.h"

// 初始化函数
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister, std::shared_ptr<LockQueue<ApplyMsg>> applyCh)
{
    m_peers = peers;
    m_persister = persister;
    m_me = me;
    m_mtx.lock();

    this->applyChan = applyCh;
    m_currentTerm = 0;
    m_status = Follower;
    
    m_commitIndex = 0;
    m_lastApplied = 0;
    m_logs.clear();
    //从1开始
    for(int i = 0;i < m_peers.size();i++){
        m_matchIndex.push_back(0);
        m_nextIndex.push_back(0);
    }
    m_votedFor = -1;
    m_lastSnapshotIncludeIndex = 0;
    m_lastSnapshotIncludeTerm = 0;
    m_lastResetElectionTime = now();
    m_lastResetHearBeatTime = now();

    //读取快照数据
    readPersist(m_persister->ReadRaftState());
    if(m_lastSnapshotIncludeIndex > 0){
        m_lastApplied = m_lastSnapshotIncludeIndex;
    }

    DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
        m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

    m_mtx.unlock();

    std::thread t1(&Raft::leaderHearBeatTicker,this);
    std::thread t2(&Raft::electionTimeOutTicker,this);
    std::thread t3(&Raft::applierTicker,this);

    t1.detach();t2.detach();t3.detach();
}

void Raft::pushMsgToKvServer(ApplyMsg msg)
{
    applyChan->Push(msg);
}

// 在节点启动的时候需要读取之前的持久化数据
void Raft::readPersist(std::string data)
{
    if(data.empty()) return;

    std::stringstream iss(data);
    boost::archive::text_iarchive ia(iss);
    BoostPersistRaftNode boostPersisterRaftNode;
    ia >> boostPersisterRaftNode;

    m_currentTerm = boostPersisterRaftNode.m_currentTerm;
    m_votedFor = boostPersisterRaftNode.m_votedFor;
    m_lastSnapshotIncludeIndex = boostPersisterRaftNode.m_lastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = boostPersisterRaftNode.m_lastSnapshotIncludeTerm;
    m_logs.clear();

    for(auto &item:boostPersisterRaftNode.m_logs){
        raftRpcProtoc::LogEntry logEntry;
        logEntry.ParseFromString(item);
        m_logs.emplace_back(logEntry);
    }
}

//kvServer主动发起,客户端的请求通过rpc打到server里然后把这个请求从这个start发到raft上
void Raft::Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader)
{
    std::lock_guard<std::mutex> lg(m_mtx);
    if(m_status != Leader){
        *newLogIndex = -1;
        *newLogTerm = -1;
        *isLeader = false;
        return;
    }

    //此时说明是leader
    raftRpcProtoc::LogEntry newLogEntry;
    //命令就是op
    newLogEntry.set_command(command.asString());
    newLogEntry.set_logindex(getNewCommandIndex());
    newLogEntry.set_logterm(m_currentTerm);
    m_logs.emplace_back(newLogEntry);

    persist();
    //相当于返回结果
    *newLogIndex = newLogEntry.logindex();
    *newLogTerm = newLogEntry.logterm();
    *isLeader = true;
}

//日志压缩快照，重新装回
void Raft::Snapshot(int index, std::string snapshot)
{
    std::lock_guard<std::mutex> lg(m_mtx);
    //快照有效性检查
    if(m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) return;
    //制作新快照
    int newLastSnapshotIncludeIndex = index;
    int newLastSnapshotIncludeTerm = m_logs[getSliceIndexFromLogIndex(index)].logterm();
    std::vector<raftRpcProtoc::LogEntry> trunckedLogs;
    //截断日志
    for(int i = index + 1;i <= getLastLogIndex();i++){
        trunckedLogs.push_back(m_logs[getSliceIndexFromLogIndex(i)]);
    }
    //更新快照信息
    m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
    //更换日志数组
    m_logs = trunckedLogs;
    //清空数组
    std::vector<raftRpcProtoc::LogEntry>().swap(trunckedLogs);
    //更新提交信息和应用信息
    m_commitIndex = std::max(m_commitIndex,index);
    m_lastApplied = std::max(m_lastApplied,index);
    //持久化
    m_persister->Save(persistData(),snapshot);
}

bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot)
{
    return true;
}

// 持久化,序列化
std::string Raft::persistData()
{
    BoostPersistRaftNode boostPersistRaftNode;
    boostPersistRaftNode.m_currentTerm = m_currentTerm;
    boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
    boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
    boostPersistRaftNode.m_votedFor = m_votedFor;
    for(auto &item:m_logs){
        //protobuf提供的序列化方法，对于LogEntry
        boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
    }

    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    //把类序列化到ss流里
    oa << boostPersistRaftNode;
    return ss.str();
}

//把raft节点持久化到文件
void Raft::persist()
{
    auto data = persistData();
    m_persister->SaveRaftState(data);
}



//* 简化版本
// 选举超时定时器
void Raft::electionTimeOutTicker()
{
    while(true){
        while(m_status == Leader){
            //对于leader来说不需要检查选举超时，这个函数会空转，因此让它睡眠
            usleep(HeartBeatTimeout);
        }
        
        // 计算随机化的选举超时时间
        auto electionTimeout = getRandomizedElectionTimeout();
        std::this_thread::sleep_for(electionTimeout);

        // 检查是否在睡眠期间重置了选举超时
        if (m_lastResetElectionTime > now() - electionTimeout) {
            // 如果重置了选举超时，则继续下一轮循环
            continue;
        }

        // 执行选举
        doElection();
    }
}

// 发起选举
void Raft::doElection()
{
    std::lock_guard<std::mutex> g(m_mtx);
    if(m_status == Leader){
        //领导者不需要选举，直接退出
        return;
    }
    
    DPrintf("[ticker-func-rf(%d)] 选举定时器到期，且不是Leader，开始选举\n",m_me);
    m_status = Candidate;
    m_currentTerm += 1;
    //既是自己个自己投也是避免candidate之间互相投
    m_votedFor = m_me;
    //投票数自己给自己投，初始为1
    std::shared_ptr<int> votedNum = std::make_shared<int>(1);
    //投票了开启新任期，要进行持久化
    persist();
    //重设定时器
    m_lastResetElectionTime = now();


    //发起RPC请求投票
    for(int i = 0;i < m_peers.size();i++){
        if(i == m_me) continue;
        //获取当前节点最后的日志信息，以便和别的raft节点比对
        int lastLogIndex = -1;
        int lastLogTerm = -1;
        getLastLogIndexAndTerm(&lastLogIndex,&lastLogTerm);
        
        //组装rpc请求
        std::shared_ptr<raftRpcProtoc::RequestVoteArgs> requestVoteArgs = std::make_shared<raftRpcProtoc::RequestVoteArgs>();
        requestVoteArgs->set_term(m_currentTerm);
        requestVoteArgs->set_lastlogindex(lastLogIndex);
        requestVoteArgs->set_lastlogterm(lastLogTerm);
        requestVoteArgs->set_candidateid(m_me);
        auto requestVoteReply = std::make_shared<raftRpcProtoc::RequestVoteReply>();

        //使用匿名函数执行，避免其拿到锁
        std::thread t(&Raft::sendRequestVote,this,i,requestVoteArgs,requestVoteReply,votedNum);
        t.detach();
    }

}

bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProtoc::RequestVoteArgs> args, std::shared_ptr<raftRpcProtoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum)
{
    auto start = now();
    DPrintf("[func-sendRequestVote rf{%d}] 向server {%d} 发送RequestVote开始",m_me,server);
    //调用util入口,发起请求
    bool ok = m_peers[server]->RequestVote(args.get(),reply.get());
    DPrintf("[func-sendRequestVote rf{%d}] 向server {%d} 发送RequestVote完毕,耗时:{%d} ms",m_me,server,now() - start);
    //处理响应
    if(!ok) return ok;
    
    std::lock_guard<std::mutex> lg(m_mtx);
    if(reply->term() > m_currentTerm){
        //候选者任期落后
        m_currentTerm = reply->term();
        m_votedFor = -1;
        m_status = Follower;
        persist();
        return true;
    }
    else if(reply->term() < m_currentTerm){
        return true;
    }

    //如果正常返回肯定会同步任期，不相等就退出
    myAssert(reply->term() == m_currentTerm,"assert reply.term == rf.currentTerm fail");

    //对方拒绝投票
    if(!reply->votegranted()) return true;

    //此时就是成功投票
    (*votedNum)++;
    if(*votedNum >= m_peers.size() / 2 + 1){
        //超过多数
        m_status = Leader;
        DPrintf("[func-sendRequestVote rf{%d}] elect success,current term:{%d},lastLogIndex:{%d}\n",m_me,m_currentTerm,getLastLogIndex());

        //更新leader维护的两个数组
        for(int i = 0;i < m_nextIndex.size();i++){
            m_nextIndex[i] = getLastLogIndex() + 1;
            m_matchIndex[i] = 0;
        }

        std::thread t(&Raft::doHeartBeat,this);
        t.detach();

        persist();
    }

    return true;
}

//接收远端发来的rpc投票请求
void Raft::RequestVote(const raftRpcProtoc::RequestVoteArgs *args, raftRpcProtoc::RequestVoteReply *reply)
{
    std::lock_guard<std::mutex> lg(m_mtx);
    
    //检查任期
    if(args->term() < m_currentTerm){
        reply->set_term(m_currentTerm);
        reply->set_votegranted(Expire);
        reply->set_votegranted(false);
        return;
    }

    if(args->term() > m_currentTerm){
        m_status = Follower;
        m_currentTerm = args->term();
        m_votedFor = -1;
        //节点状态变更，进行持久化操作
        persist();
    }

    //如果任期相等，还要比较日志
    if(!UpToDate(args->lastlogindex(),args->lastlogterm())){
        //此时说明对方发来的日志旧
        reply->set_term(m_currentTerm);
        reply->set_votegranted(false);
        reply->set_votestate(Voted);
        return;
    }

    //如果日志小于对方,检查是否已经投过了
    if(m_votedFor = -1&&m_votedFor != args->candidateid()){
        reply->set_term(m_currentTerm);
        reply->set_votegranted(false);
        reply->set_votestate(Voted);
        return;
    }

    //此时可以投了
    m_votedFor = args->candidateid();
    m_lastResetElectionTime = now();
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);
}

//leader的心跳定时器
void Raft::leaderHearBeatTicker()
{
    while(true){
        //如果不是leader不能在这空转消耗性能，用条件变量控制
        std::unique_lock<std::mutex> lock(m_mtx);
        m_cv.wait(lock,[this]{return m_status == Leader;});

        auto wakeTime = std::chrono::system_clock::now();
        //计算睡眠时间
        auto sleepDuration = std::chrono::milliseconds(HeartBeatTimeout) - (wakeTime - m_lastResetHearBeatTime);

        if(sleepDuration > std::chrono::milliseconds(0)){
            std::this_thread::sleep_for(sleepDuration);
        }

        lock.unlock();

        if(wakeTime - m_lastResetHearBeatTime < std::chrono::milliseconds(HeartBeatTimeout)){
            continue;//定时器未超时，继续下一轮循环
        }

        doHeartBeat();//执行心跳操作
    }
}

void Raft::leaderSendSnapShot(int server)
{
    std::unique_lock<std::mutex> lk(m_mtx);
    raftRpcProtoc::InstallSnapshotRequest args;
    args.set_leaderid(m_me);
    args.set_term(m_currentTerm);
    args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
    args.set_data(m_persister->ReadSnapshot());

    raftRpcProtoc::InstallSnapshotResponse reply;
    lk.unlock();
    bool ok = m_peers[server]->InstallSnapshot(&args,&reply);
    lk.lock();
    if(!ok) return;

    if(m_status != Leader || m_currentTerm != args.term()) return;

    if(reply.term() > m_currentTerm){
        m_currentTerm = reply.term();
        m_votedFor = -1;
        m_status = Follower;
        persist();
        m_lastResetElectionTime = now();
        return;
    }

    m_matchIndex[server] = args.lastsnapshotincludeindex();
    m_nextIndex[server] = m_matchIndex[server] + 1; 
}

//leader构造心跳包
void Raft::doHeartBeat()
{
    if(m_status != Leader) return;
    std::lock_guard<std::mutex> lock(m_mtx);
    DPrintf("[func-Raft::doHeartBeat()-Leader:{%d}] Leader的心跳定时器触发了且拿到了mutex,开始发送AE\n",m_me);
    auto appendNums = std::make_shared<int>(1);//正确返回的节点数量

    //对Follower(除自己外的所有节点发送AE)
    for(int i = 0;i < m_peers.size();i++){
        if(i == m_me){
            continue;
        }

        myAssert(m_nextIndex[i] >= 1,format("rf.nextIndex[%d] = {%d}",i,m_nextIndex[i]));
        //日志压缩加入后要判断是发送快照还是发送AE
        if(m_nextIndex[i] <= m_lastSnapshotIncludeIndex){
            std::thread t(&Raft::leaderSendSnapShot,this,i);
            t.detach();
            continue;
        }

        //构造RPC请求
        int preLogIndex = -1;//nextIndex - 1
        int preLogTerm = -1;
        getPrevLogInfo(i,&preLogIndex,&preLogTerm);

        std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> appendEntriesArgs = std::make_shared<raftRpcProtoc::AppendEntriesArgs>();
        appendEntriesArgs->set_term(m_currentTerm);
        appendEntriesArgs->set_leaderid(m_me);
        appendEntriesArgs->set_prevlogindex(preLogIndex);
        appendEntriesArgs->set_prevlogterm(preLogTerm);
        appendEntriesArgs->clear_entries();
        appendEntriesArgs->set_leadercommit(m_commitIndex);

        //* 两种添加日志的方式
        if(preLogIndex != m_lastSnapshotIncludeIndex){
            for(int j = getSliceIndexFromLogIndex(preLogIndex) + 1;j < m_logs.size();++j){
                //添加日志条目
                raftRpcProtoc::LogEntry *sendEntryPtr = appendEntriesArgs->add_entries();
                *sendEntryPtr = m_logs[j];
            }
        }
        else{
            //说明直接同步快照后的所有日志条目就行了
            for(const auto &item:m_logs){
                raftRpcProtoc::LogEntry *sendEntryPtr = appendEntriesArgs->add_entries();
                *sendEntryPtr = item;
            }
        }

        int lastLogIndex = getLastLogIndex();
        //确保从prevLogIndex发到最后
        assert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex);

        //构造返回值
        const std::shared_ptr<raftRpcProtoc::AppendEntriesReply> appendEntriesReply = std::make_shared<raftRpcProtoc::AppendEntriesReply>();

        appendEntriesReply->set_appstate(Disconnected);

        //由sendAppendEntries发起真正的RPC请求
        std::thread t(&Raft::sendAppendEntries,this,i,appendEntriesArgs,appendEntriesReply,appendNums);
        t.detach();
    }
    //重置心跳时间
    m_lastResetHearBeatTime = now();
}

//对远端的RPC节点发起AE请求
bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args, std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums)
{
    bool ok = m_peers[server]->AppendEntries(args.get(),reply.get());
    if(!ok){
        DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader向节点{%d}发送AE rpc失败",m_me,server);
        return ok;
    }

    //此时虽然发送成功，但是对方断连
    if(reply->appstate() == Disconnected) return ok;

    std::lock_guard<std::mutex> lock(m_mtx);

    //对reply进行处理
    if(reply->term() > m_currentTerm){
        m_status = Follower;
        m_currentTerm = reply->term();
        m_votedFor = -1;
        persist();
        return ok;
    }
    else if(reply->term() < m_currentTerm){
        //考虑打印
        return ok;
    }

    //此时term相等
    assert(reply->term() == m_currentTerm);

    if(!reply->success()){
        //* 日志不匹配,-100只是特殊标记，无意义
        if(reply->updatenextindex() != -100){
            //对方期望收到的index
            m_nextIndex[server] = reply->updatenextindex(); //失败是不更新match的
        }
    }
    else{
        *appendNums = *appendNums + 1;//成功返回的节点数量 + 1
        DPrintf("---------------------tmp-------------------节点{%d} 返回true,当前*appendNums{%d}",server,*appendNums);

        m_matchIndex[server] = std::max(m_matchIndex[server],args->prevlogindex() + args->entries_size());
        m_nextIndex[server] = m_matchIndex[server] + 1;
        int lastLogIndex = getLastLogIndex();

        assert(m_nextIndex[server] <= lastLogIndex + 1);

        if(*appendNums >= 1 + m_peers.size() / 2){
            //此时半数以上的节点正常返回，可以commit了
            *appendNums = 0;

            //* 领导人完备性
            //* 只有当前任期的日志条目可以被领导者直接提交。
            //* 对于之前任期的日志条目，领导者必须先确保当前任期有日志条目被提交，然后才能间接地提交之前任期的日志条目。
            if(args->entries_size() > 0&&args->entries(args->entries_size() - 1).logterm() == m_currentTerm){
                //至少保证不比当前的提交索引小
                m_commitIndex = std::max(m_commitIndex,args->prevlogindex() + args->entries_size());
            }

            assert(m_commitIndex <= lastLogIndex);
        }
    }
    
    return ok;
}

//本地追加日志方法
void Raft::AppendEntries1(const raftRpcProtoc::AppendEntriesArgs *args, raftRpcProtoc::AppendEntriesReply *reply)
{
    std::lock_guard<std::mutex> locker(m_mtx);
    reply->set_appstate(AppNormal);

    if(args->term() < m_currentTerm){
        //leader过期
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(-100);
        return;
    }

    if(args->term() > m_currentTerm){
        m_status = Follower;
        m_currentTerm = args->term();
        m_votedFor = -1;
    }
    //如果同一个term收到了leader的AE需要变成follower
    m_status = Follower;
    m_lastResetElectionTime = now();

    //此时term相等，比较日志
    if(args->prevlogindex() > getLastLogIndex()){
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(getLastLogIndex()+1);
        return;
    }
    else if(args->prevlogindex() < m_lastSnapshotIncludeIndex){
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);
    }

    //日志匹配成功（长度和任期都匹配）
    if(matchLog(args->prevlogindex(),args->prevlogterm())){
        //开始追加日志
        for(int i = 0;i < args->entries_size();i++){
            auto log = args->entries(i);
            if(log.logindex() > getLastLogIndex()){
                //超过就直接添加日志
                m_logs.push_back(log);
            }
            else{
                //没超过就比较是否匹配，不匹配在更新，而不是截断
                if(m_logs[getSliceIndexFromLogIndex(log.logindex())].logterm() == log.logterm()&&
                m_logs[getSliceIndexFromLogIndex(log.logindex())].command() != log.command())
                {
                    //* 相同位置的log，logTerm相等，但是命令却不相同，不符合raft的前向匹配，异常了退出
                    return;
                }

                //*领导者更迭可能导致索引一致，但是term不一致
                if(m_logs[getSliceIndexFromLogIndex(log.logindex())].logterm() != log.logterm()){
                    //* 不匹配就换成领导者的日志条目进行覆盖
                    m_logs[getSliceIndexFromLogIndex(log.logindex())] = log;
                }
            }
        }

        //追加完日志后,因为可能会收到过期log，因此这里是大于等于
        assert(getLastLogIndex() >= args->prevlogindex() + args->entries_size());

        if(args->leadercommit() > m_commitIndex){
            //看leader  commit到哪了
            m_commitIndex = std::min(args->leadercommit(),getLastLogIndex());
        }

        reply->set_success(true);
        reply->set_term(m_currentTerm);
    }
    else{
        //* 长度匹配，但是任期不匹配
        //优化
        //PrevLogIndex长度合适但是不匹配，说明term不匹配，直接默认此term内所有的日志都不匹配

        reply->set_updatenextindex(args->prevlogindex());
        //* 加速匹配
        for(int index = args->prevlogindex();index >= m_lastSnapshotIncludeIndex;--index){
            //* 找到term不匹配的任期就返回(就是越过term相同的日志条目)
            if(getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())){
                reply->set_updatenextindex(index + 1);
                break;
            }
        }

        reply->set_success(false);
        reply->set_term(m_currentTerm);

        return;
    }


}

//* 定时写入状态机
void Raft::applierTicker()
{
    while(true){
        m_mtx.lock();
        //拿到应该应用的log
        auto applyMsgs = getApplyLogs();
        m_mtx.unlock();

        for(auto &message:applyMsgs){
            applyChan->Push(message);
        }
        sleepNMilliseconds(ApplyInterval);
    }
    
}

void Raft::InstallSnapshot(const raftRpcProtoc::InstallSnapshotRequest *args, raftRpcProtoc::InstallSnapshotResponse *reply)
{
    std::lock_guard<std::mutex> lk(m_mtx);
    if(args->term() < m_currentTerm){
        reply->set_term(m_currentTerm);
        return;
    }
    if(args->term() > m_currentTerm){
        m_currentTerm = args->term();
        m_votedFor = -1;
        m_status = Follower;
        persist();
    }

    m_status = Follower;
    m_lastResetElectionTime = now();
    //如果快照过期
    if(args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) return;

    //截断日志，修改commit和lastApplied
    auto lastLogIndex = getLastLogIndex();
    if(lastLogIndex > args->lastsnapshotincludeindex()){
        m_logs.erase(m_logs.begin(),m_logs.begin()+getSliceIndexFromLogIndex(args->lastsnapshotincludeindex()+1));
    }
    else{
        m_logs.clear();
    }

    m_commitIndex = std::max(m_commitIndex,args->lastsnapshotincludeindex());
    m_lastApplied = std::max(m_lastApplied,args->lastsnapshotincludeindex());
    m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
    m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();

    reply->set_term(m_currentTerm);
    ApplyMsg msg;
    msg.SnapshotValid = true;
    msg.Snapshot = args->data();
    msg.SnapshotTerm = args->lastsnapshotincludeterm();
    msg.CommandIndex = args->lastsnapshotincludeindex();

    applyChan->Push(msg);
    std::thread t(&Raft::pushMsgToKvServer,this,msg);
    t.detach();

    m_persister->Save(persistData(),args->data());
}

//* ===================================RPC方法=============================================
void Raft::AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProtoc::AppendEntriesArgs *request, ::raftRpcProtoc::AppendEntriesReply *response, ::google::protobuf::Closure *done)
{
    AppendEntries1(request,response);
    done->Run();
}

void Raft::InstallSnapshot(google::protobuf::RpcController *controller, const ::raftRpcProtoc::InstallSnapshotRequest *request, ::raftRpcProtoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done)
{
    InstallSnapshot(request,response);
    done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProtoc::RequestVoteArgs *request, ::raftRpcProtoc::RequestVoteReply *response, ::google::protobuf::Closure *done)
{
    RequestVote(request,response);
    done->Run();
}

//* ======================================辅助函数===========================================
void Raft::getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm)
{
    //为空说明被快照保存了
    if(m_logs.empty()){
        *lastLogIndex = m_lastSnapshotIncludeIndex;
        *lastLogTerm = m_lastSnapshotIncludeTerm;
        return;
    }
    //不为空则提取
    *lastLogIndex = m_logs[m_logs.size()-1].logindex();
    *lastLogTerm = m_logs[m_logs.size()-1].logterm();
}

//循环应用状态机
std::vector<ApplyMsg> Raft::getApplyLogs()
{
    std::vector<ApplyMsg> applyMsgs;

    while (m_lastApplied < m_commitIndex) {
        m_lastApplied++;
        ApplyMsg applyMsg;
        applyMsg.CommandValid = true;
        applyMsg.SnapshotValid = false;
        applyMsg.Command = m_logs[getSliceIndexFromLogIndex(m_lastApplied)].command();
        applyMsg.CommandIndex = m_lastApplied;
        applyMsgs.emplace_back(applyMsg);
    }
    return applyMsgs;
}

// 新来日志的index生成
int Raft::getNewCommandIndex()
{
    auto lastLogIndex = getLastLogIndex();
    return lastLogIndex + 1;
}


// 检查发来的rpc日志是否比当前节点新
bool Raft::UpToDate(int index, int term)
{
    int lastIndex = -1;
    int lastTerm = -1;
    getLastLogIndexAndTerm(&lastIndex,&lastTerm);
    return term > lastTerm||(term == lastTerm && index >= lastIndex);
}


// leader调用，传入:服务器index，传出:发送AE的preLogIndex和PrevLogTerm
void Raft::getPrevLogInfo(int server, int *preIndex, int *preTerm)
{
    if(m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1){
        //要发送的日志是第一个日志，直接返回快照
        *preIndex = m_lastSnapshotIncludeIndex;
        *preTerm = m_lastSnapshotIncludeTerm;
        return;
    }

    auto nextIndex = m_nextIndex[server];
    *preIndex = nextIndex - 1;
    //有了快照之后，日志索引就不能和下标相等了，因为被压缩了，所以需要根据索引找到实际的日志
    *preTerm = m_logs[getSliceIndexFromLogIndex(*preIndex)].logterm();
}

void Raft::GetState(int *term, bool *isLeader)
{
    std::lock_guard<std::mutex> lk(m_mtx);

    *term = m_currentTerm;
    *isLeader = (m_status == Leader);
}

//leader确保哪些日志已经被正常应用到其他raft节点
void Raft::leaderUpdateCommitIndex()
{
    m_commitIndex = m_lastSnapshotIncludeIndex;
    
    for(int index = getLastLogIndex();index >= m_lastSnapshotIncludeIndex + 1;index--){
        int sum = 0;
        for(int i = 0;i < m_peers.size();i++){
            if(i == m_me){
                sum += 1;
                continue;
            }
            if(m_matchIndex[i] >= index){
                sum += 1;
            }
        }

        if(sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm){
            m_commitIndex = index;
            break;
        }
    }

}

int Raft::getSliceIndexFromLogIndex(int logIndex)
{
    //找到index对应的真实下标
    myAssert(logIndex > m_lastSnapshotIncludeIndex,format("[func-getSlicesIndexFromLogIndex-rf{%d}] index{%d} <= rf.lastSnapshotIncludeIndex{%d}",m_me,
                    logIndex,m_lastSnapshotIncludeIndex));
    int lastLogIndex = getLastLogIndex();
    assert(logIndex <= lastLogIndex);

    //减去快照索引再-1，因为数组从0开始
    int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;

    return SliceIndex;
}

//看看日志是否匹配
bool Raft::matchLog(int logIndex, int logTerm)
{
    assert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex());
    
    return logTerm == getLogTermFromLogIndex(logIndex);
}

//根据index获得term
int Raft::getLogTermFromLogIndex(int logIndex)
{
    assert(logIndex >= m_lastSnapshotIncludeIndex);
    int lastLogIndex = getLastLogIndex();

    assert(logIndex <= lastLogIndex);
    if(logIndex == m_lastSnapshotIncludeIndex){
        return m_lastSnapshotIncludeTerm;
    }
    else{
        //获得实际在数组中的位置的term
        return m_logs[getSliceIndexFromLogIndex(logIndex)].logterm();
    }
}

//如果raft日志数超过这个数量就需要制作快照了
int Raft::GetRaftStateSize()
{
    //调用持久层中的，一旦raft节点调用persist就会更新size,因为共用一个持久层
    return m_persister->RaftStateSize();
}

int Raft::getLastLogIndex()
{
    int lastLogIndex = -1;
    int _ = -1;
    getLastLogIndexAndTerm(&lastLogIndex, &_);
    return lastLogIndex;
}

int Raft::getLastLogTerm() {
  int _ = -1;
  
  int lastLogTerm = -1;
  getLastLogIndexAndTerm(&_, &lastLogTerm);
  return lastLogTerm;
}