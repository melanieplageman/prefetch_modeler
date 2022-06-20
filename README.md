# prefetch_modeler

## Setup

To create a python venv with the required dependencies run
```shell
make sdist
```

## Running the built-in prefetch model with built-in storage types and workloads.

The command line utility `weir` is provided to run prefetch simulations. It
takes a file containing definitions of simulations and displays along with
optional arguments for the duration of the simulation and the number of IOs to
process.

`test.py` is an example of the definition file to provide to `weir`. You can
use this one or create your own.

Using `test.py`, you would run `weir` like this:

```
./weir test.py --duration=10 --volume=100
```

## Customizing the weir input file

The `weir` input file should contain `Simulation`s and `ChartGroup`s.
`Simulation`s define what to run. `ChartGroup`s define how to display the
output of running the simulation.

### Simulation

Create one or more `Simulation` instances with all the components of the
prefetch simulator pipeline in order:

1. prefetcher
2. rate limiter
3. storage type buckets (as many as needed to model the storage)
4. workload buckets (as many as needed to model the workload)

The `Simulation` is a pipeline that will run each of these buckets in order.

Each component should be an individual class which subclasses `Bucket`.

```
simulation = Simulation(Prefetcher, RateLimiter, *storage_buckets, *wl_buckets)
```

The `weir` input file should create an instance of a `Simulation` and associate
it with a `ChartGroup` but should not run the `Simulation`.

### Display Components

`weir` expects one main component in the input file: a list called `output`
containing `ChartGroup` instances.

```
output = [Group1(simulation1), Group2(simulation2)]
```

`ChartGroup`s are associated with a single `Simulation` and display the output
from running this simulation. `ChartGroup`s contain multiple `Chart`s, each
made with `ChartType` and containing various metrics.

### Examples

Depending on what you would like to compare you will set up the `Simulation`s
and `ChartGroup`s differently.

For example, if you would like to compare the demand rate and IO latency over
time of two different prefetchers with the same workload and storage types,
first you create two `Simulation` instances:

```
simulation1 = Simulation(Prefetcher1, RateLimiter1, *storage1, *wl1)
simulation2 = Simulation(Prefetcher2, RateLimiter1, *storage1, *wl1)
```

Then, define a `ChartGroup` subclass containing `Chart`s (returned by
`ChartType`) with the desired metrics. Use `MetaChartGroup` as the metaclass
for the class you define:

```
class Group1(ChartGroup, metaclass=MetaChartGroup):
    Latency = ChartType(io_latency)
    Demand = ChartType(demand_rate)
```

You can add any number of `Chart`s (using `ChartType`) to the `ChartGroup`
subclass. You can also make the `Chart`s outside of the `ChartGroup` subclass
definition and reuse them across `ChartGroup` subclasses.

```
Drain = ChartType(remaining, done)

class Group1(ChartGroup, metaclass=MetaChartGroup):
    Latency = ChartType(io_latency)
    Drain = Drain

class Group2(ChartGroup, metaclass=MetaChartGroup):
    Demand = ChartType(demand_rate)
    Drain = Drain
```

Each metric passed to `ChartType` should be defined in the metrics catalog in
`metric.py`. You can define new metrics in this file.

Finally, associate `Simulation`s with instances of your `ChartGroup` subclass
and add them to the list of `ChartGroup`s called `output`:

```
output = [Group1(simulation1), Group1(simulation2)]
```

If you want to have different `Chart`s on each `ChartGroup`, you can make
multiple `ChartGroup` subclasses and use different `ChartGroup` subclasses for
each `Simulation`.

```
output = [Group1(simulation1), Group2(simulation2)]
```

You can also associate the same simulation with multiple `ChartGroup`s. It will
be re-run for each `ChartGroup` and the correct metrics collected.

```
output = [Group1(simulation1), Group2(simulation1)]
```

## Customization

### Making your own storage models.

### Making your own workload types.

### Making your own prefetchers and rate limiters.
