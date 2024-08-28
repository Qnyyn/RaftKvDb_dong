#include "kvServer.h"
#include <rpcprovider.h>
#include "mprpcconfig.h"

kvServer::kvServer(int me, int maxraftstate, std::string nodeInfoFileName, short port)
{
    std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);
    m_me = me;
    m_maxRaftState = maxraftstate;
    applyChan = std::make_shared<LockQueue<ApplyMsg>>();
    m_raftNode = std::make_shared<Raft>();
    //clerk 层面kvServe开启rpc接收功能
    std::thread t([this,port]()->void{
        //provider是一个rpc网络服务对象,把UserService对象发布到rpc节点上
        RpcProvider provider;
        provider.NotifyService(this);
        provider.NotifyService(this->m_raftNode.get()); //这里获取了原始指针
        provider.Run(m_me,port);
    });
    t.detach();

    //这里要保证所有节点都开启了rpc接收功能之后才能开启rpc远程调用能力(因为要连接所有rpc节点)
    sleep(6);

    //获取所有raft节点的ip，port，并进行连接，排除自己
    //方便测试，先不用Zookeeper
    MprpcConfig config;
    config.LoadConfigFile(nodeInfoFileName.c_str());
    std::vector<std::pair<std::string,short>> ipPortVt;
    for(int i = 0;i < INT_MAX - 1;++i){
        std::string node = "node" + std::to_string(i);

        std::string nodeIp = config.Load(node + "ip");
        std::string nodePortStr = config.Load(node + "port");
        if(nodeIp.empty()){
            break;
        }
        ipPortVt.emplace_back(nodeIp,atoi(nodePortStr.c_str()));
    }
    std::vector<std::shared_ptr<RaftRpcUtil>> servers;


    //开始连接
    for(int i = 0;i < ipPortVt.size();i++){
        if(i == me){
            servers.push_back(nullptr);
            continue;
        }
        //拿到ip和端口，用rpcUtil连接
        std::string otherNodeIp = ipPortVt[i].first;
        short otherNodePort = ipPortVt[i].second;
        auto *rpc = new RaftRpcUtil(otherNodeIp,otherNodePort);
        servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));
        //此时就是连接成功
    }
    //等待所有节点都连接成功后启动raft
    sleep(ipPortVt.size() - me);
    m_raftNode->init(servers,m_me,persister,applyChan);

    //* kv的server直接与raft通信，但kv（也就是跳表）不直接与raft通信，所以需要把ApplyMsg的chan传下去用于通信，两者的persist也是共用的
    m_skipList;
    waitApplyCh;
    m_lastRequestId;
    m_lastSnapShotRaftLogIndex = 0;
    auto snapshot = persister->ReadSnapshot();
    if(!snapshot.empty()){
        ReadSnapShotToInstall(snapshot);
    }
    std::thread t2(&kvServer::ReadRaftApplyCommandLoop,this); //t2循环卡在这不会结束
    t2.join();

}



//获取raft已经提交的日志条目
void kvServer::GetCommandFromRaft(ApplyMsg message)
{
    Op op;
    op.parseFromString(message.Command);

    if(message.CommandIndex <= m_lastSnapShotRaftLogIndex){
        return;
    }

    if(!ifRequestDuplicate(op.ClientId,op.RequestId)){
        //不重复则执行命令，应用到状态机
        if(op.Operation == "Put"){
            ExecutePutOpOnKvDb(op);
        }
        if(op.Operation == "Append"){
            ExecuteAppendOpOnKvDb(op);
        }
    }

    //到这里kvDB已经制作了快照
    if(m_maxRaftState != -1){
        IfNeedToSendSnapShotCommand(message.CommandIndex,9);
        //如果raft的log大于指定的比例，我们就把它制作成快照应用到raft节点
    }

    //Send message to the chan of op.ClientId
    SendMessageToWaitChan(op,message.CommandIndex);
}



bool kvServer::SendMessageToWaitChan(const Op &op, int raftIndex)
{
    std::lock_guard<std::mutex> lg(m_mtx);
    if(waitApplyCh.find(raftIndex) == waitApplyCh.end()){
        return false;
    }

    waitApplyCh[raftIndex]->Push(op);
    return true;
}


//* ====================持久化相关(与Raft节点通信)==============================
// 检查是否需要发送快照到Raft节点
//todo                                                    这个参数好像没用到
void kvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion)
{
    if(m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0){
        //Send Snapshot Command,作用是日志压缩，提高状态恢复的效率
        auto snapshot = MakeSnapShot();
        m_raftNode->Snapshot(raftIndex,snapshot);
    }
}

//由循环接收raft节点的ReadRaftApplyCommandLoop调用
void kvServer::GetSnapShotFromRaft(ApplyMsg message)
{
    std::lock_guard<std::mutex> lg(m_mtx);
    if(m_raftNode->CondInstallSnapshot(message.SnapshotTerm,message.SnapshotIndex,message.Snapshot)){
        ReadSnapShotToInstall(message.Snapshot);
        m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
    }
}

std::string kvServer::MakeSnapShot()
{
    std::lock_guard<std::mutex> lg(m_mtx);
    std::string snapshotData = getSnapShotData();//内嵌的序列化函数，当前跳表存储引擎序列化
    return snapshotData;
}



//从快照中读取
void kvServer::ReadSnapShotToInstall(std::string snapshot)
{
    if(snapshot.empty()) return;
    //这个函数里调用了load，加载到跳表引擎中
    parseFromString(snapshot);
}

//* ==========================辅助函数======================================
//检查请求是否重复，确保线性一致性
bool kvServer::ifRequestDuplicate(std::string clientId, int requestId)
{
    std::lock_guard<std::mutex> lg(m_mtx);
    if(m_lastRequestId.find(clientId) == m_lastRequestId.end()){
        //不存在返回不重复
        return false;
    }
    //如果requestId <= m_lastRequestId[clientId]说明请求和之前重复，因为一个clientId对应的requestId是线性增长的
    return requestId <= m_lastRequestId[clientId];
}

void kvServer::DprintfKvDb()
{
    if(!Debug) return;
    std::lock_guard<std::mutex> lg(m_mtx);
    m_skipList.display_list();
}

//* ============================对跳表存储引擎的操作(简单)======================================
void kvServer::ExecuteAppendOpOnKvDb(Op op)
{
    m_mtx.lock();

    m_skipList.insert_set_element(op.Key, op.Value);
    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();
}

void kvServer::ExecuteGetOpOnKvDb(Op op, std::string *value, bool *exist)
{
    m_mtx.lock();
    *value = "";
    *exist = false;
    if(m_skipList.search_element(op.Key,*value)){
        *exist = true;
        //已经对value赋值完成了
    }

    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();
}

void kvServer::ExecutePutOpOnKvDb(Op op)
{
    m_mtx.lock();
    m_skipList.insert_element(op.Key,op.Value);
    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();
}

//* ===========================RPC函数======================================

void kvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request, ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done)
{
    //调用本地函数
    kvServer::PutAppend(request,response);
    //done回调自动发回对端
    done->Run();
}

void kvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request, ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done)
{
    kvServer::Get(request,response);
    done->Run();
}

//*==========================RPC调用的本地函数================================
void kvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply)
{
    Op op{args->op(),args->key(),args->value(),args->clientid(),args->requestid()};
    int raftIndex = -1;
    bool isLeader = false;
    m_raftNode->Start(op,&raftIndex,nullptr,&isLeader);

    if(!isLeader){
        reply->set_err(ErrWrongLeader);
        return;
    }

    std::unique_lock<std::mutex> lock(m_mtx);
    //获取或创建对应的日志索引的等待队列
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    auto chForRaftIndex = waitApplyCh[raftIndex];
    lock.unlock();

    Op raftCommitOp;
    if(!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT,&raftCommitOp)){
        //* 和get差在这里，因为超时了不能像get一样直接获取
        //* put会违反线性一致性
        if(ifRequestDuplicate(op.ClientId,op.RequestId)){
            reply->set_err(OK);//超时了，但是重复请求，返回ok
        }
        else{
            reply->set_err(ErrWrongLeader);
        }
    }
    else{
        //没超时
        if(raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId){
            reply->set_err(OK);
        }
        else{
            reply->set_err(ErrWrongLeader);
        }
    }

    lock.lock();
    auto tmp = waitApplyCh[raftIndex];
    waitApplyCh.erase(raftIndex);
    delete tmp;
    lock.unlock();
}

//处理来自客户端的Get请求
void kvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply)
{
    Op op = {"Get",args->key(),"",args->clientid(),args->requestid()};

    //启动raft操作，获取日志索引以及领导状态，顺便把Get请求打到raft上
    int raftIndex = -1;
    bool isLeader = false;
    m_raftNode->Start(op,&raftIndex,nullptr,&isLeader);
    //客户端请求失败，不是leader，客户端应该处理重试别的节点
    if(!isLeader){
        reply->set_err(ErrWrongLeader);
        return;
    }

    //使用锁来保护等待队列的访问
    std::unique_lock<std::mutex> lock(m_mtx);
    //获取或创建对应的日志索引的等待队列
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    auto chForRaftIndex = waitApplyCh[raftIndex];
    lock.unlock();

    
    //等待Raft应用操作，设置超时时间
    Op raftCommitOp;
    if(!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT,&raftCommitOp)){
        //超时了还没获得操作的话，看看是不是以前的Get操作，那样可以直接获得也不违反线性一致性
        //如果Get请求重复直接在kv数据库操作就行
        if(ifRequestDuplicate(op.ClientId,op.RequestId)){
            std::string value;
            bool exist = false;
            ExecuteGetOpOnKvDb(op,&value,&exist);
            reply->set_err(exist?OK:ErrNoKey);
            reply->set_value(value);
        }
        else{
            //让client重新试试
            reply->set_err(ErrWrongLeader);
        }
    }
    else{
        //如果raft已经提交操作，则站在KV数据库上执行Get操作
        if(raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId){
            std::string value;
            bool exist = false;
            ExecuteGetOpOnKvDb(op,&value,&exist);
            reply->set_err(exist?OK:ErrNoKey);
            reply->set_value(value);
        }
        else{
            reply->set_err(ErrWrongLeader);
        }
    }

    //清理队列
    lock.lock();
    delete waitApplyCh[raftIndex];
    waitApplyCh.erase(raftIndex);
    lock.unlock();

}

//*  kvServer启动就会调用，循环等待获取raft发来的ApplyMsg
void kvServer::ReadRaftApplyCommandLoop()
{
    while(true){
        auto message = applyChan->Pop();
        if(message.CommandValid){
            GetCommandFromRaft(message);
        }
        if(message.SnapshotValid){
            GetSnapShotFromRaft(message);
        }
    }
}
