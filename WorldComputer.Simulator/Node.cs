using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorldComputer.Simulator
{
    
    public  class Node
    {
        public int Number { get; init; } = 0;
        public int ProcessID { get; set; } = 0;
        
        public Node() { }

        public Node(int nodeNumber ) 
        { 
            Number = nodeNumber;
        }
    }
}
