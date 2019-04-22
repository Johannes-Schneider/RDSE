# Tue, 16.04.2019

## 002 Full Meeting

__Topic:__
* Distributed Model Training

__Next Steps:__

1. Write a short document (exposé) about what we are going to do (key notes)
- _Motivation:_ What is this topic about; Why is is interesting
- _Related Work:_ focus on as few papers as possible
- _Approach:_ What are we going to do; Concept?!
- _Evaluation:_ Which dataset might fit; Which challenges are going to be tackled? - How can we evaluate that?
- _References:_  

__Time schedule:__
- _Intermediate Presentation:_ mid semester
- _Final Presentation:_ beginning July
- _Paper:_ mid - end August (overall flexible)

__Akka Tips & Tricks:__
- _Large Messages:_ Artery -> [__StreamRef__](https://doc.akka.io/docs/akka/current/stream/stream-refs.html)
- _Serialization:_ Start with __Kryo__ (as it is very simple); Later think about using different serializers for better performance