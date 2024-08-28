#include "Persister.h"
#include "util.h"

void Persister::Save(std::string raftstate,std::string snapshot){
    std::lock_guard<std::mutex> lg(m_mtx);
    clearRaftStateAndSnapshot();
    //将raftstate和snapshot写入本地文件
    m_raftStateOutStream << raftstate;
    m_snapshotOutStream << snapshot;
}
std::string Persister::ReadSnapshot(){
    std::lock_guard<std::mutex> lg(m_mtx);
    if(m_snapshotOutStream.is_open()){
        m_snapshotOutStream.close();
    }

    DEFER{
        //延迟代码，不管怎么样都会打开
        m_snapshotOutStream.open(m_snapshotFileName);//默认追加
    };

    std::fstream ifs(m_snapshotFileName,std::ios_base::in);
    if(!ifs.good()){
        return "";
    }

    std::string snapshot;
    ifs >> snapshot;
    ifs.close();
    return snapshot;
}
//写入raft节点的状态
void Persister::SaveRaftState(const std::string &data){
    std::lock_guard<std::mutex> lg(m_mtx);
    clearRaftState();
    m_raftStateOutStream << data;
    m_raftStateSize += data.size();
}
long long Persister::RaftStateSize(){
    std::lock_guard<std::mutex> lg(m_mtx);
    return m_raftStateSize;
}

//读取raft节点状态
std::string Persister::ReadRaftState(){
    std::lock_guard<std::mutex> lg(m_mtx);
    
    std::fstream ifs(m_raftStateFileName,std::ios_base::in);
    if(!ifs.good()) return "";

    std::string raftstate;
    ifs >> raftstate;
    ifs.close();
    return raftstate;
}
Persister::Persister(int me)
        :m_raftStateFileName("raftstatePersist" + std::to_string(me) + ".txt"),
         m_snapshotFileName("snapshotPersist" + std::to_string(me) + ".txt"),
         m_raftStateSize(0){
        
    //检查文件状态并清空文件
    bool fileOpenFlag = true;
    std::fstream file(m_raftStateFileName,std::ios::out|std::ios::trunc);
    if(file.is_open()) file.close();
    else fileOpenFlag = false;

    file = std::fstream(m_snapshotFileName,std::ios::out|std::ios::trunc);
    if(file.is_open()) file.close();
    else fileOpenFlag = false;

    //两个都打开且清空才算成功
    if(!fileOpenFlag){
        DPrintf("[func-Persister::Persister] file open error");
    }

    //绑定流
    m_raftStateOutStream.open(m_raftStateFileName);
    m_snapshotOutStream.open(m_snapshotFileName);
}
Persister::~Persister(){
    if(m_raftStateOutStream.is_open()){
        m_raftStateOutStream.close();
    }
    if(m_snapshotOutStream.is_open()){
        m_snapshotOutStream.close();
    }
}

void Persister::clearRaftState(){
    m_raftStateSize = 0;
    //关闭文件流
    if(m_raftStateOutStream.is_open()){
        m_raftStateOutStream.close();
    }
    //重新打开文件流并清空文件内容
    m_raftStateOutStream.open(m_raftStateFileName,std::ios::out|std::ios::trunc);
}
void Persister::clearSnapshot(){
    //关闭文件流
    if(m_snapshotOutStream.is_open()){
        m_snapshotOutStream.close();
    }
    //重新打开文件流并清空文件内容
    m_snapshotOutStream.open(m_snapshotFileName,std::ios::out|std::ios::trunc);
}
void Persister::clearRaftStateAndSnapshot(){
    clearRaftState();
    clearSnapshot();
}