//Lucas Mehmel 
//Lab1 Sept 26
//Description: This calculates the averages of process speeds for the baby nodes. 
//0 node is the head node that calculates the averages and recieves messages from the baby nodes. 
//All other nodes are baby nodes and send a message to the head node.
#include <mpi.h>
#include <iostream>

using namespace std;

int main(int argc, char *argv[])
{
	//Initialize
	MPI::Init(argc,argv);
	
	//Number of times to each node
	int copies = 10000;
	//placeholder
	int data = 0;	
	
	// number of baby nodes
	int size = MPI::COMM_WORLD.Get_size() - 1;
	
	// the array to mark when each node comes in
	int order [size];
	for (int m = 0; m < size; m++)
	{
		order[m] = 0;
	}	
 
	// Main part of the program where the nodes communicate
	for(int i = 0; i < copies; i++)
	{
		// syncronize nodes
		MPI::COMM_WORLD.Barrier();
		//gets the rank of the current node
		int rank = MPI::COMM_WORLD.Get_rank();

		MPI::Status stat;
		// if head node
		if(rank == 0)
		{
			//head node has to recieve messages for each baby node
			for (int c = 0; c < size; c++)
			{
				//Head node receives message from baby node
				MPI::COMM_WORLD.Recv(&data, 1, MPI::INT, MPI::ANY_SOURCE, MPI::ANY_TAG, stat);
				// gets baby node rank
				int baby = stat.Get_source();
				// sets the position or index in the array 
				int position = baby - 1;
				// puts the position the node responded into the array 
				order [position] += (c + 1); 
			}
		}
		// if not head node
		else{
			//send message to head node
			MPI::COMM_WORLD.Send(&data, 1, MPI::INT, 0, rank);
		}
	}	
	// after going through all the copies and at head node
	if (MPI::COMM_WORLD.Get_rank() == 0)
	{
		//for each baby node print out the average response order
		for (int n = 0; n < size; n++)
		{
			cout<<"For rank "<<n+1<<" the average response order is "<<((double)order[n])/copies<<endl;
		}
	}
	//Finalize
	MPI::Finalize();
	//returns 0 because it can
	return 0;
}
