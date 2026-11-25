#import "@preview/faithful-acmart:0.2.0": *

#show: acmart.with(
  format: "sigconf",
  title: "Detecting Trading Trends in Financial Tick Data",
  subtitle: "CS-E4780 Scalable Systems and Data Management Course Project",
  conference: (
    name: "CS-E4780 Scalable Systems and Data Management",
    short: "CS-E4780",
    date: "November 2026",
    venue: "Espoo, Finland",
  ),
  copyright: "none",
  doi: none,
  isbn: none,
  print-acm-reference: false,
  authors: (
    (
      name: "Joona Kärkkäinen",
      affiliation: (
        institution: "Aalto University",
        city: "Espoo",
        country: "Finland",
      ),
    ),
  ),
  abstract: [
    TODO
  ],
  keywords: ("stream processing", "complex event processing", "Apache Flink", "Apache Kafka"),
)

= Introduction

TODO: background and overview of the project @debs2022data.

= System Architecture

TODO: design choices and motivations.

= Implementation

TODO: hardware and cloud services, programming tools, GUI tools.

= Performance Evaluation

== Correctness

TODO

== Scalability

TODO

== Resource Utilization

TODO

= Conclusion

TODO

// Group projects: state each member's contributions.
// = Contributions

#bibliography("refs.bib")
