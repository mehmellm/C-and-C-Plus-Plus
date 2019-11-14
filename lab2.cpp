//Lucas Mehmel
//Lab 2 
//This program takes in 4 datasets. then takes one data set at a time and sorts the words from smallest to largest. it then outputs the document number of the word, the word number, and the number of times that word
//appears in the at document.


#include <mpi.h>
#include <iostream>
#include <fstream>
#include <limits>

using namespace std;

struct Entry
{
    int docNum, wordNum, wordCount;
};

//gets index of smallest element
int smallestIndex(Entry arr[], int size)
{
    Entry temp;
    int smallest = 0;

    if (arr[0].wordCount != 0)
    {
        temp = arr[0];
    }
    else
    {
        Entry e;
        e.wordNum = numeric_limits<int>::max();
        temp = e;
    }

    for (int i = 1; i < size; i++)
    {
        if (arr[i].wordNum != 0)
        {
            if (temp.wordNum > arr[i].wordNum)
            {
                temp = arr[i];
                smallest = i;
            }
            else if (temp.wordNum == arr[i].wordNum)
            {
                if (temp.wordCount < arr[i].wordCount)
                {
                    temp = arr[i];
                    smallest = i;
                }
            }
        }
    }
    return smallest;
}

// checks to see if all elements in an entry are 0
bool notAllNull(Entry arr[], int size)
{
    bool clear = false;
    for (int i = 0; i < size; i++)
    {
        if (arr[i].wordNum != 0)
        {
            clear = true;
            return clear;
        }
    }
    return clear;
}

// swaps 2 entries
void swap(Entry *a, Entry *b)
{
	Entry t = *a;
        *a = *b;
        *b = t;
}

//Helper function for partition
bool lessThan(Entry one, Entry two)
{
	if (one.wordNum < two.wordNum)
	{
		return true;
	}
	else if (one.wordNum == two.wordNum && one.wordCount > two.wordCount)	
	{
		return true;
	}
	return false;
}

//quicksort helper
int partition(Entry arr[], int low, int high)
{
    Entry pivot = arr[low];
    int i = (low);

    for (int j = low + 1; j <= high; j++)
    {
        if (lessThan(arr[j], pivot))
        {       
	        i += 1;
            swap(&arr[i], &arr[j]);	
	}
    }
    swap(&arr[low], &arr[i]);
    return i;
}

//sorts array of words
void quickSort(Entry arr[], int low, int high)
{
    if (low < high)
    {
        int pi = partition(arr, low, high);
        quickSort(arr, low, pi - 1);
        quickSort(arr, pi + 1, high);
    }
}

// Main part of program
int main(int argc, char *argv[])
{
	// total number of documents
	int x;

	//intialize status
	MPI::Status status;

	//Initialize
	MPI::Init(argc,argv);
	
	//number of baby nodes
	int size = MPI::COMM_WORLD.Get_size() - 1;
        
	//gets the rank of the current node
	int rank = MPI::COMM_WORLD.Get_rank();
	
	string datasetName;
	string docWord;
	string docStart;	
	string outputFile;


	//this sets the dataset and runs through all 4 datasets
	for (int i = 0; i < 4 ; i++)
	{
		if( i == 0)
		{
			datasetName = "ENRON";
			docWord = "/home/mcconnel/BagOfWords/docword.enron.txt";
			docStart = "/home/mcconnel/BagOfWords/docstart.enron.txt";
			outputFile = "OutputENRON.txt";
		} 	
	
		if( i == 1)
                {
                        datasetName = "NIPS";
                        docWord = "/home/mcconnel/BagOfWords/docword.nips.txt";
                        docStart = "/home/mcconnel/BagOfWords/docstart.nips.txt";
			outputFile = "OutputNIPS.txt";
                }
		
		if( i == 2)
                {
                        datasetName = "KOS";
                        docWord = "/home/mcconnel/BagOfWords/docword.kos.txt";
                        docStart = "/home/mcconnel/BagOfWords/docstart.kos.txt";
			outputFile = "OutputKOS.txt";
                }

		if( i == 3)
                {
                        datasetName = "NYTIMES";
                        docWord = "/home/mcconnel/BagOfWords/docword.nytimes.txt";
                        docStart = "/home/mcconnel/BagOfWords/docstart.nytimes.txt";
			outputFile = "OutputNYTimes.txt";
                }

		// the array of which node gets what documents
		int docNum[size];	
	
		// array that has the start and finish document number that the babies will use
		int bbarray[2];

		if(rank == 0)
		{
			const char *dWord = docWord.c_str();
			// reads in the file
			ifstream infile;
			infile.open(dWord);
			infile >> x;
			
			//prints what dataset is being sorted
			cout << datasetName << endl;
			
			// used to get the number to increment by
			int increment = x / size;

			//this loop puts the number of the document that each node will end on 
			for (int i = 0; i < size; i++)
			{
				//at last node then it will end on the last document
				if (i == size - 1)
				{	
					docNum[i] = x;
				}
				// increment for each node
				else
				{
					docNum[i] = increment;
					increment = x / size + increment;
				}
			} 

			//this puts into an array the start and finish document number and sends to baby nodes
			for (int i = 0; i < size; i++)
			{	
				if (i == 0 )
				{
					int start = 1;
					int finish = docNum[i];
					bbarray[0] = start;
					bbarray[1] = finish;
				}
				else
				{
					int start = docNum[i-1] + 1;
					int finish = docNum[i];
					bbarray[0] = start;
					bbarray[1] = finish;
				}	
				MPI::COMM_WORLD.Send(bbarray, 2, MPI::INT, i + 1, rank);
			}

			MPI::COMM_WORLD.Barrier();

			bool done = false;
			int mem = 0;
	                Entry data;
			//store the smallest value from each baby nodes
	                Entry tempEnt[size];
   		        int baby;
		        int message = 1;
	                int ind = 0;

			//initialize temp Entry
		        for (int i = 0; i < size; i++)
		        {
		                MPI::COMM_WORLD.Recv(&data, 3, MPI::INT, MPI::ANY_SOURCE, MPI::ANY_TAG, status);
		                baby = status.Get_source() - 1;
		               tempEnt[baby] = data;
		        }

	                ofstream outfile(outputFile.c_str());

			//find the position of smallest element, send a message to baby node, sends smallest element
		        int smallest;
		        smallest = smallestIndex(tempEnt, size);
			//outputs to file
	                outfile << tempEnt[smallest].docNum << " " << tempEnt[smallest].wordNum << " " << tempEnt[smallest].wordCount << endl;
//			cout << tempEnt[smallest].docNum << " " << tempEnt[smallest].wordNum << " " << tempEnt[smallest].wordCount << endl;

            		MPI::COMM_WORLD.Send(&message, 1, MPI::INT, smallest + 1, rank);

			//finds smallest element and has baby node send smallest, does this till no more elements
		        while (!done)
		        {
				MPI::COMM_WORLD.Recv(&data, 3, MPI::INT, MPI::ANY_SOURCE, MPI::ANY_TAG, status);
		                baby = status.Get_source() - 1;
                	        tempEnt[baby] = data;
		                smallest = smallestIndex(tempEnt, size);
		         
			        if (notAllNull(tempEnt, size))
		                {
					MPI::COMM_WORLD.Send(&message, 1, MPI::INT, smallest + 1, rank);
				}
               			else
	                	{
					done = true;
	                	}

        	        	if (tempEnt[smallest].docNum != 0)
                		{
					outfile << tempEnt[smallest].docNum << " " << tempEnt[smallest].wordNum << " " << tempEnt[smallest].wordCount << endl;
//					cout << tempEnt[smallest].docNum << " " << tempEnt[smallest].wordNum << " " << tempEnt[smallest].wordCount << endl;
	                	}
			}
			outfile.close();
	            	cout << "Done!" << endl;
		}
	
		//if rank not = 0
		else
		{		
			//Recieves the array of how many documents to view
			MPI::COMM_WORLD.Recv(bbarray, 2, MPI::INT, MPI::ANY_SOURCE, MPI::ANY_TAG, status);
			
			ifstream dStart;
            		dStart.open(docStart.c_str());

		        int num = 0;
		        int bite = 0;
	                int index = 0;
    	                int result[2];

			//find the binary number for starting and ending document to read
		        while (!dStart.eof())
               	        {
			        dStart >> num >> bite;
                		if (num == bbarray[index])
               		        {
		                    result[index] = bite;
                		    index = index + 1;
		                }
            	        }
		        dStart.close();

                        ifstream wordFile;
                        wordFile.open(docWord.c_str());
            		
		        //Document number
			int docNumber = 0;
			//Word Number
		        int wordNum = 0;
			//Word count
		        int wordCount = 0;
	                int counter = 0;
	                
			//set the pointer to read the document
			wordFile.seekg(result[0]);

			//counts number of vocab
            		while ((wordFile >> docNumber >> wordNum >> wordCount) && (docNumber <= bbarray[1]))
            		{
		                counter++;
		        }
	                
			dStart.close();
            	        
			// creates an array of size counter. the size of the number of words		        
			Entry *entries = new Entry[counter];
	                
			counter = 0;
           		
			ifstream dWord;
		        dWord.open(docWord.c_str());
			// goes to the start of the baby's first document
		        dWord.seekg(result[0]);
             		
			//get the documnet number, word number, and word count and store it to the entries
			while ((dWord >> docNumber >> wordNum >> wordCount) && (docNumber <= bbarray[1]))
	                {
        		        Entry e;
		                e.docNum = docNumber;
                		e.wordNum = wordNum;
		                e.wordCount = wordCount;
                		entries[counter] = e;
		 	        counter++;	
		   	}
	                
			dWord.close();
        	        
			//sorts the arrary
			quickSort(entries, 0, counter - 1);

			MPI::COMM_WORLD.Barrier();
			int mem;
			Entry entry;

			//send a smallest elemnet and if it recieve the message, send the next smallest element. repeat until no more elements
			for (int n = 0; n < counter; n++)
            		{
		                entry = entries[n];
		                MPI::COMM_WORLD.Send(&entry, 3, MPI::INT, 0, rank);
		                MPI::COMM_WORLD.Recv(&mem, 1, MPI::INT, MPI::ANY_SOURCE, MPI::ANY_TAG, status);
		        }

			//send fake to say its the end
		        Entry fake;
		        fake.docNum = 0;
		        fake.wordNum = 0;
		        fake.wordCount = 0;
	                MPI::COMM_WORLD.Send(&fake, 3, MPI::INT, 0, rank);
	        }
	    }
	//Finalize	
	MPI::Finalize();
}

