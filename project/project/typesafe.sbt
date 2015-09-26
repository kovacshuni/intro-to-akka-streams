val rpUrl = "https://dl.bintray.com/typesafe/subscribers-early-access"
val rpVersion = "15v09p01-instrumented-01"

resolvers += "typesafe-rp-mvn" at rpUrl
resolvers += Resolver.url("typesafe-rp-ivy", url(rpUrl))(Resolver.ivyStylePatterns)

addSbtPlugin("com.typesafe.rp" % "sbt-typesafe-rp" % rpVersion)
