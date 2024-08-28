#pragma once
#include<string>

//raft节点与kvServer沟通的作用
class ApplyMsg{
public:
    //true 为有效的日志条目，需要被应用到状态机
    bool CommandValid;
    //真正的命令，通常是键值对操作
    std::string Command;
    //命令在日志中的索引位置
    int CommandIndex;

    bool SnapshotValid;
    std::string Snapshot;
    int SnapshotTerm;
    int SnapshotIndex;

public:
    ApplyMsg():CommandValid(false),Command(),CommandIndex(-1),SnapshotValid(false),Snapshot(),SnapshotTerm(-1),SnapshotIndex(-1){}
};