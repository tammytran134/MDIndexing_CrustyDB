# The Performance Engineering Milestone

This milestone is a bit different than previous. In the first milestone you had specific instructions and suggestions to complete it, while in the second and third we gave you more freedom. In this milestone, your goal is to beat the performance of our baseline solution. You have many ways to try to beat our baseline, and will likely require performance engineering or implementing some ideas we have discussed in class. 

## Beating baseline performance 

In this milestone, you'll work on speeding up your database in any way you want, although we recommend you focus your efforts on the performance bottlenecks, because you'll be evaluated in your ability to beat our reference implementation's performance (well, not really, your code just needs to be within +-5% of our implementation's performance).

You can start by trying a few queries on your own using the performance benchmarks we have provided. 

We are working to integrate the performance results in Gradescope so you can see where you are when you submit a version to gradescope.

#### Instructions to run performance benchmarks

Make sure you've pulled the latest version. Then, in the root directory you should see two folders, 'test_data' and 'e2e-benchmarks'. 'test_data' contains a few csv files that our default benchmark code uses to test your database's performance. We provide them so you can try the benchmarking code easily, but we encourage you to include other data to try out the system's performance. The 'e2e-benchmarks' contains the crate with all the necessary tooling to run the benchmarks. You can easily run them by doing:

```bash
>$ cd e2e-benchmarks
>$ cargo bench -p e2e-benchmarks
```

This will start a client and a server automatically for you (assuming your code compiles and your database runs correctly), it will silence the server so it does not send the query results to the client (we are only interested in measuring the query performance), and then conduct a series of benchmarks you can see in:

```bash
e2e-benchmarks/benches/benchmarks/joinbench.rs
```

Take a look at the Criterion crate [documentation](https://bheisler.github.io/criterion.rs/book/getting_started.html) to understand how it works and to learn how to interpret the output of the benchmarks.

Finally, because the benchmarks start a client and server for you, if they do not finish properly, for example, because your database throws an error, then the server process may be left running in the background. Because it's bound to a port, you won't be able to rerun the database until you kill the process. In Unix-based systems you can locate the crusty process by doing something like:

```bash
>$ ps aux | grep crusty
```

then collect the PID and kill the process by doing:

```bash
>$ kill PID
```

#### Grading

We'll run your code on data we create and compare its performance with our reference implementation performance. You'll get 80% of the points if the performance of your database is within 5 points of our reference implementation's performance. If you beat our implementation by more than 5 points then you'll get all points. We expect every student of the class will be able to beat our database implementation performance. How to achieve that, however, may be not obvious. Study the database bottlenecks and invest your time and effort wisely.

This milestone requires a write-up if you have matched or exceeded our performance. Your write up (my-perf.txt) should contain:

 -  A brief description of your solution. In particular, include what design
decisions you made and explain why. This is only needed for those parts of your
solutions that had some significant design work (e.g. what you did to make your
database fast). 

- How long you roughly spent on the milestone, and what would have
liked/disliked on the milestone.

Not having the write-up can cause you to lose up to 5% points on this milestone!

### How to Approach

There are several ways to make your code more performant than our baseline. One approach is to profile your code (potentially even writing new criterion tests) and optimize expensive functions. Raul will offer a dedicated OH going over how to approach this. You could also try to add functionality to accelerate the queries. This could include things like indexes, adding a buffer pool, optimize your query plan, and more.

Query optimizers are among the most complex software in RDBMS. Among the many operations they are responsible for, join reordering is a particularly important one from a performance perspective. If you take a look at the performance benchmarks (see the point above), you'll see a join_right and join_left functions. These functions perform the exact same query, just swapping the order in which joins are fed. You've learned in class why join ordering is important. Here, you have a chance to work on reordering the joins before the physical plan is executed by the executor component of CrustyDB. One approach can be considering a (limited) optimizer for this milestone.

## Scoring and Requirements

We encourage you to think through the requirements of this milestone and choose what to work on wisely. But also we encourage you to choose whatever seems more fun. This is the last milestone!
