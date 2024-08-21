# HederaP2P
Hedera24 Hello Future Hackathon Repo

Welcome to the HederP2P submission repo readme for the Hedera 24 Hello Future Hacathon Repo.   This repo contains only the source code that was added to the broader UnoSys decentralized operating project which is not open source and is not provided here.  All changes to the UnoSys code base for this hackathon were net new additions and all such additions have included here.

Please see this video link for an explanation of the project and demonstrations of the software that is being submitted:

https://youtu.be/hORPAKG6-T0


Please use this link to download the software itself (it was too big to be placed on GitHub).  As its P2P software you must download it and run it locally.  This isn't the "web" anymore.

https://worldcomputer.org/Documents/WhitePapers/HederaP2p.zip

# NOTES
The above video link shows two demos.  The first one you can reproduce yourself on a Windows machine immediatley after downloading the software.  This will demonstrate a P2P node incombination with the Hedera Consensus Service (running on the TestNet and using the Test MirrorNet ) self-organizing into a mesh network clusters of P2P nodes.  The 2nd demo - the one that demonstrates a Virtual Drive, requires a free 3rd party download from CallBack Technologies (https://www.callback.com/).  The WorldComputer.Simulator software heavily modifies one of the developer samples that comes with the CBFS Connect 2024 .NET Edition  (see https://github.com/RodDaSilvaWCO/HederaP2P/blob/main/WorldComputer.Simulator/VDrive.cs) to create a VDrive implmenetation backed by UnoSys and Hedera Consensus Services.  If you wish to try out the VDrive as demonstrated for yourself you need to download CBFS Connect 2024 .NET Edition and install it on your computer first.  CallBack Technologies provides a free 30 day trial version that can be downloaded here:

https://www.callback.com/download/download?sku=CCNJ-A&type=demo

Note once you do this, the first time you use the WCSIM VD /CREATE command you will be asked to install a driver.  It is a user (not a kernal) mode driver from Callback Technolgies and does not require your computer to reboot, and is perfectly safe.

The product is excellent for anyone wishing to implement a Virtual Drive (whether for Windows, Linux or Mac) backed by some custom data store, which is exactly what I used it for in this submission - to build a Virtual Drive backed by a P2P network.

I would like to give a shout out to Jason Fabritz for his awesome Hashgraph .NET SDK product which I used once again (I have used it the last two Hedera hackathons as well) to integrate UnoSys (which is entirely written in .NET) with the Hedera Consensus Service.  Its an excellent piece of work and Hedera should take a serious look at supporting it and the .NET developer community (6 million strong) better.

If you are interested in learning more about UnoSys or the broader World Computer Project that it is apart of, there is more information included whitepapers to be found at www.WorldComputer.org.

Finally, I can be reached at RodDaSilva@WorldComputer.org if there are any questions.

Have fun!


