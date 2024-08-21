using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorldComputer.Simulator
{
    [Serializable]
    public class NetworkSpec
    {
        public List<Node> NodeList { get; init; } = new List<Node>();

        public NetworkSpec() { }
    }
}
