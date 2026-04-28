package nl.postparcel.tracking

import org.slf4j.Logger
import org.slf4j.LoggerFactory

inline val <reified T : Any> T.logger get(): Logger = LoggerFactory.getLogger(T::class.java)
