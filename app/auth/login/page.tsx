"use client";

import Image from "next/image";
import { useRouter } from "next/navigation";
import { useState, useEffect } from "react";

const AuthLogin: React.FC = () => {
  const router = useRouter();

  return (
    <div className="min-h-screen min-w-screen flex flex-col mx-auto bg-[var(--background)] rounded-lg">
      <div className="flex justify-center w-full h-full my-auto xl:gap-14 lg:justify-normal md:gap-5 draggable">
        <div className="flex items-center justify-center w-full lg:p-12">
          <div className="flex items-center xl:p-10">
            <form className="flex flex-col w-full h-full pb-6 text-center bg-[var(--background)] rounded-3xl">
              <h3 className="mb-3 text-4xl font-extrabold text-[var(--foreground)]">
                Sign In
              </h3>
              <p className="mb-4 text-[var(--secondary)]">Enter your email and password</p>
              <div className="flex flex-row gap-2">
                <a 
                className="flex flex-row items-center justify-center w-full py-2 text-sm font-medium transition duration-300 rounded-2xl 
                text-[var(--foreground)] bg-[var(--light-background)] hover:bg-[var(--dark-background)] focus:ring-4 
                focus:ring-[var(--secondary)] hover:cursor-pointer"
                >
                  <Image width={256} height={256} className="aspect-square border-none w-7" src={require('@/app/assets/icons/google.svg')} alt="" />
                  {/* <h1 className="mt-1">Google</h1> */}
                </a>
                <a 
                className="flex flex-row items-center justify-center w-full py-2  text-sm font-medium transition duration-300 rounded-2xl 
                text-[var(--foreground)] bg-[var(--light-background)] hover:bg-[var(--dark-background)] focus:ring-4 
                focus:ring-[var(--secondary)] hover:cursor-pointer"
                >
                  <Image width={256} height={256} className="aspect-square border-none w-7 " src={require('@/app/assets/icons/github.svg')} alt="" />
                  {/* <h1 className="mt-1">Github</h1> */}
                </a>
              </div>
              <div className="flex items-center mb-3">
                <hr className="h-0 border-b border-solid border-[var(--secondary)] grow" />
                <p className="mx-4 text-[var(--foreground)]">or</p>
                <hr className="h-0 border-b border-solid border-[var(--secondary)] grow" />
              </div>
              <label htmlFor="email" className="mb-2 text-sm text-start text-[var(--foreground)]">Email*</label>
              <input id="email" type="email" placeholder="admin@jugadbase.com" className="flex items-center w-full px-5 pb-3 pt-4 mr-2 text-sm font-medium outline-none focus:bg-[var(--secondary)] mb-5 placeholder:text-[var(--foreground)] bg-[var(--light-background)] text-[var(--foreground)] rounded-2xl"/>
              <label htmlFor="password" className="mb-2 text-sm text-start text-[var(--foreground)]">Password*</label>
              <input id="password" type="password" placeholder="Enter a password" className="flex items-center w-full px-5 pb-3 pt-4 mb-5 mr-2 text-sm font-medium outline-none focus:bg-[var(--secondary)] placeholder:text-[var(--foreground)] bg-[var(--light-background)] text-[var(--foreground)] rounded-2xl"/>
              <div className="flex flex-row justify-between mb-2">
                <label className="relative inline-flex items-center mr-3 cursor-pointer select-none">
                  <span className="ml-3 text-sm font-normal text-[var(--foreground)]">Keep me logged in</span>
                </label>
                <a onClick={() => {router.push("/auth/reset-password")}} className="mr-4 text-sm font-medium text-[var(--tertiary)]">Forget password?</a>
              </div>
              <button className="w-full px-6 py-5 mb-5 text-sm font-bold leading-none text-white transition duration-300 md:w-96 rounded-2xl hover:bg-[var(--tertiary)] focus:ring-4 focus:ring-[var(--tertiary)] bg-[var(--primary)]">Sign In</button>
              <p className="text-sm leading-relaxed text-[var(--foreground)]">Not registered yet? <a href="javascript:void(0)" className="font-bold text-[var(--secondary)]">Create an Account</a></p>
            </form>
          </div>
        </div>
      </div>
    </div>
  );
}

export default AuthLogin;
